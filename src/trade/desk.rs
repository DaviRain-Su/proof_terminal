use std::sync::Arc;

use gpui::{
    App, Bounds, ClickEvent, Context, CursorStyle, Entity, FocusHandle, Focusable, FontWeight,
    InteractiveElement, IntoElement, KeyDownEvent, ListAlignment, ListState, MouseButton,
    MouseDownEvent, MouseMoveEvent, MouseUpEvent, ParentElement, PinchEvent, Pixels, Render,
    ScrollDelta, ScrollWheelEvent, SharedString, Stateful, Styled, Window, canvas, div, fill, list,
    point, prelude::*, px, relative, size,
};
use proof_phoenix::{DeskSnapshot, MarketListing, Timeframe, window_candles};

use super::format;
use super::store::{FeedCommand, FeedEvent, spawn_feed};
use crate::theme::Theme;
use crate::ui::ActivationExt;

const BOOK_DEPTH: usize = 12;
const BOOK_ROW_HEIGHT: f32 = 18.0;
const TRADE_ROW_HEIGHT: f32 = 18.0;
const MARKET_ROW_HEIGHT: f32 = 28.0;
const HEADER_HEIGHT: f32 = 48.0;

pub struct ProofDesk {
    focus: FocusHandle,
    commands: crossbeam_channel::Sender<FeedCommand>,
    selected_symbol: String,
    timeframe: Timeframe,
    markets: Arc<Vec<MarketListing>>,
    desk: Option<Arc<DeskSnapshot>>,
    last_error: Option<String>,
    stale: bool,
    candles: Arc<Vec<proof_phoenix::Candle>>,
    book_ask_list: ListState,
    book_bid_list: ListState,
    trade_list: ListState,
    market_list: ListState,
    ticket_side_long: bool,
    ticket_is_limit: bool,
    header_drag_armed: bool,
    chart_visible: usize,
    chart_offset: usize,
    chart_drag: Option<(f32, usize)>,
}

impl ProofDesk {
    pub fn new(window: &mut Window, cx: &mut App) -> Entity<Self> {
        let (commands, events) = spawn_feed();
        let entity = cx.new(|cx| {
            let focus = cx.focus_handle();
            Self {
                focus,
                commands,
                selected_symbol: proof_phoenix::DEFAULT_SYMBOL.to_owned(),
                timeframe: Timeframe::OneMinute,
                markets: Arc::new(Vec::new()),
                desk: None,
                last_error: None,
                stale: false,
                candles: Arc::new(Vec::new()),
                book_ask_list: ListState::new(0, ListAlignment::Bottom, px(48.0)),
                book_bid_list: ListState::new(0, ListAlignment::Top, px(48.0)),
                trade_list: ListState::new(0, ListAlignment::Top, px(48.0)),
                market_list: ListState::new(0, ListAlignment::Top, px(80.0)),
                ticket_side_long: true,
                ticket_is_limit: true,
                header_drag_armed: false,
                chart_visible: 80,
                chart_offset: 0,
                chart_drag: None,
            }
        });

        let focus = entity.read(cx).focus.clone();
        window.focus(&focus, cx);

        let feed = entity.downgrade();
        cx.spawn(async move |cx| {
            while let Ok(event) = events.recv().await {
                if feed
                    .update(cx, |this, cx| this.apply_event(event, cx))
                    .is_err()
                {
                    break;
                }
            }
        })
        .detach();

        entity
    }

    fn apply_event(&mut self, event: FeedEvent, cx: &mut Context<Self>) {
        match event {
            FeedEvent::Markets(markets) => {
                if self.markets.len() != markets.len() {
                    self.market_list.reset(markets.len());
                }
                self.markets = Arc::new(markets);
            }
            FeedEvent::Desk(snapshot) => {
                if snapshot.symbol != self.selected_symbol || snapshot.timeframe != self.timeframe {
                    return;
                }
                self.last_error = None;
                self.stale = false;
                self.candles = snapshot.candles.clone();
                self.sync_lists(&snapshot);
                self.desk = Some(Arc::from(snapshot));
            }
            FeedEvent::Error(error) => {
                self.last_error = Some(error);
                if self.desk.is_some() {
                    self.stale = true;
                }
            }
        }
        cx.notify();
    }

    fn sync_lists(&mut self, snapshot: &DeskSnapshot) {
        set_list_len(
            &self.book_ask_list,
            snapshot.book.asks.len().min(BOOK_DEPTH),
        );
        set_list_len(
            &self.book_bid_list,
            snapshot.book.bids.len().min(BOOK_DEPTH),
        );
        set_list_len(&self.trade_list, snapshot.trades.len());
    }

    fn select_symbol(&mut self, symbol: String, cx: &mut Context<Self>) {
        if symbol == self.selected_symbol {
            return;
        }
        self.selected_symbol = symbol.clone();
        self.stale = false;
        self.last_error = None;
        self.chart_offset = 0;
        let _ = self.commands.send(FeedCommand::Select {
            symbol,
            timeframe: self.timeframe,
        });
        cx.notify();
    }

    fn set_timeframe(&mut self, timeframe: Timeframe, cx: &mut Context<Self>) {
        if timeframe == self.timeframe {
            return;
        }
        self.timeframe = timeframe;
        self.chart_offset = 0;
        let _ = self.commands.send(FeedCommand::Select {
            symbol: self.selected_symbol.clone(),
            timeframe,
        });
        cx.notify();
    }

    fn handle_key(&mut self, event: &KeyDownEvent, cx: &mut Context<Self>) {
        if event.keystroke.modifiers.modified() {
            return;
        }
        match event.keystroke.key.as_str() {
            "1" => self.select_symbol("SOL".into(), cx),
            "2" => self.select_symbol("BTC".into(), cx),
            "3" => self.select_symbol("ETH".into(), cx),
            "=" | "+" => self.zoom_chart(true, cx),
            "-" => self.zoom_chart(false, cx),
            "left" => self.pan_chart(8, cx),
            "right" => self.pan_chart(-8, cx),
            "home" | "0" => self.reset_chart(cx),
            _ => {}
        }
    }

    fn zoom_chart(&mut self, zoom_in: bool, cx: &mut Context<Self>) {
        let len = self.candles.len().max(1);
        let mut visible = self.chart_visible as f32;
        if zoom_in {
            visible *= 0.85;
        } else {
            visible *= 1.18;
        }
        let next = (visible.round() as usize).clamp(20, len.max(20));
        let next_offset = self.chart_offset.min(len.saturating_sub(next));
        if next == self.chart_visible && next_offset == self.chart_offset {
            return;
        }
        self.chart_visible = next;
        self.chart_offset = next_offset;
        cx.notify();
    }

    fn pan_chart(&mut self, bars: isize, cx: &mut Context<Self>) {
        let len = self.candles.len();
        let visible = self.chart_visible.clamp(20, len.max(20));
        let max_offset = len.saturating_sub(visible) as isize;
        let next = (self.chart_offset as isize + bars).clamp(0, max_offset) as usize;
        if next == self.chart_offset {
            return;
        }
        self.chart_offset = next;
        cx.notify();
    }

    fn reset_chart(&mut self, cx: &mut Context<Self>) {
        if self.chart_visible == 80 && self.chart_offset == 0 && self.chart_drag.is_none() {
            return;
        }
        self.chart_visible = 80;
        self.chart_offset = 0;
        self.chart_drag = None;
        cx.notify();
    }

    fn end_chart_drag(&mut self, cx: &mut Context<Self>) {
        if self.chart_drag.take().is_some() {
            cx.notify();
        }
    }

    fn connection_label(&self) -> (&'static str, bool) {
        if self.last_error.is_some() && self.desk.is_none() {
            ("offline", true)
        } else if self.stale {
            ("stale", true)
        } else if self.desk.as_ref().is_some_and(|desk| {
            desk.mark.is_some() || !desk.book.bids.is_empty() || !desk.candles.is_empty()
        }) {
            ("live", false)
        } else {
            ("connecting", false)
        }
    }
}

impl Drop for ProofDesk {
    fn drop(&mut self) {
        let _ = self.commands.send(FeedCommand::Shutdown);
    }
}

impl Focusable for ProofDesk {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus.clone()
    }
}

impl Render for ProofDesk {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = Theme::current(cx);
        let title = format!("Proof Terminal — {}-PERP", self.selected_symbol);
        window.set_window_title(&title);

        let snapshot = self.desk.clone();
        let candles = self.candles.clone();
        let selected = self.selected_symbol.clone();
        let timeframe = self.timeframe;
        let (status_label, status_warn) = self.connection_label();
        let error = self.last_error.clone();

        div()
            .key_context("ProofDesk")
            .track_focus(&self.focus)
            .size_full()
            .flex()
            .flex_col()
            .bg(theme.canvas)
            .text_color(theme.text)
            .font_family(".SystemUIFont")
            .on_action(|_: &crate::CloseWindow, window, _| crate::platform::hide_window(window))
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, _, cx| {
                this.handle_key(event, cx);
            }))
            .on_mouse_move(cx.listener(Self::on_chart_drag))
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(|this, _: &MouseUpEvent, _, cx| this.end_chart_drag(cx)),
            )
            .capture_any_mouse_up(cx.listener(|this, _: &MouseUpEvent, _, cx| {
                this.end_chart_drag(cx);
            }))
            .child(self.render_top_bar(
                window,
                snapshot.as_deref(),
                status_label,
                status_warn,
                theme,
                cx,
            ))
            .child(
                div()
                    .flex_1()
                    .min_h(px(0.0))
                    .flex()
                    .child(self.render_markets(&selected, theme, cx))
                    .child(self.render_chart_and_tables(
                        snapshot.as_deref(),
                        &candles,
                        timeframe,
                        theme,
                        cx,
                    ))
                    .child(self.render_book_and_ticket(snapshot.as_deref(), theme, cx)),
            )
            .child(self.render_bottom(snapshot.as_deref(), error.as_deref(), theme))
    }
}

impl ProofDesk {
    fn render_top_bar(
        &self,
        window: &Window,
        snapshot: Option<&DeskSnapshot>,
        status_label: &'static str,
        status_warn: bool,
        theme: Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let left_controls = self.render_window_controls_left(window, cx);
        let right_controls = self.render_window_controls_right(window, cx);
        let mark = snapshot.and_then(|desk| desk.mark);
        let change = snapshot.and_then(|desk| desk.change_24h_pct);
        let change_up = change.unwrap_or(0.0) >= 0.0;
        let symbol = snapshot
            .map(|desk| desk.symbol.as_str())
            .unwrap_or(self.selected_symbol.as_str());
        let name = snapshot
            .map(|desk| desk.name.as_str())
            .unwrap_or("Phoenix Perp");

        self.titlebar_drag(
            div()
                .id("proof-header")
                .h(px(HEADER_HEIGHT))
                .flex_none()
                .flex()
                .items_center()
                .px(px(12.0))
                .gap(px(12.0))
                .border_b_1()
                .border_color(theme.border)
                .bg(theme.surface)
                .children(left_controls)
                .child(
                    div()
                        .flex()
                        .items_baseline()
                        .gap(px(8.0))
                        .child(
                            div()
                                .text_size(px(15.0))
                                .font_weight(FontWeight::SEMIBOLD)
                                .child(SharedString::from(format!("{symbol}-PERP"))),
                        )
                        .child(
                            div()
                                .text_size(px(12.0))
                                .text_color(theme.text_tertiary)
                                .child(SharedString::from(name.to_owned())),
                        ),
                )
                .child(stat_chip(
                    "Mark",
                    mark.map(format::price).unwrap_or_else(|| "—".into()),
                    theme.text,
                    theme,
                ))
                .child(stat_chip(
                    "24h",
                    change
                        .map(format::signed_percent)
                        .unwrap_or_else(|| "—".into()),
                    if change_up {
                        theme.success
                    } else {
                        theme.danger
                    },
                    theme,
                ))
                .child(stat_chip(
                    "OI",
                    snapshot
                        .and_then(|desk| desk.open_interest)
                        .map(format::compact)
                        .unwrap_or_else(|| "—".into()),
                    theme.text_secondary,
                    theme,
                ))
                .child(stat_chip(
                    "Fund",
                    snapshot
                        .and_then(|desk| desk.funding_pct)
                        .map(format::unsigned_percent)
                        .unwrap_or_else(|| "—".into()),
                    theme.text_secondary,
                    theme,
                ))
                .child(stat_chip(
                    "Vol",
                    snapshot
                        .and_then(|desk| desk.volume_24h)
                        .map(format::compact)
                        .unwrap_or_else(|| "—".into()),
                    theme.text_secondary,
                    theme,
                ))
                .child(div().flex_1())
                .child(
                    div()
                        .px(px(8.0))
                        .h(px(22.0))
                        .rounded(px(6.0))
                        .bg(theme.overlay)
                        .flex()
                        .items_center()
                        .text_size(px(11.0))
                        .text_color(if status_warn {
                            theme.warning
                        } else {
                            theme.success
                        })
                        .child(SharedString::from(status_label)),
                )
                .child(
                    div()
                        .px(px(8.0))
                        .h(px(22.0))
                        .rounded(px(6.0))
                        .border_1()
                        .border_color(theme.border_strong)
                        .flex()
                        .items_center()
                        .text_size(px(11.0))
                        .text_color(theme.text_secondary)
                        .child("Paper"),
                )
                .children(right_controls),
            cx,
        )
    }

    fn render_window_controls_left(
        &self,
        _window: &Window,
        _cx: &mut Context<Self>,
    ) -> Option<gpui::AnyElement> {
        None
    }

    fn render_window_controls_right(
        &self,
        _window: &Window,
        _cx: &mut Context<Self>,
    ) -> Option<gpui::AnyElement> {
        None
    }

    fn render_markets(
        &self,
        selected: &str,
        theme: Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let entity = cx.weak_entity();
        let rows = self.markets.clone();
        let selected = selected.to_owned();
        div()
            .w(px(188.0))
            .h_full()
            .flex_none()
            .flex()
            .flex_col()
            .border_r_1()
            .border_color(theme.border)
            .bg(theme.sidebar_drag_background)
            .child(section_label("Markets", theme))
            .child(
                div().flex_1().min_h(px(0.0)).child(
                    list(self.market_list.clone(), move |index, _, cx| {
                        let Some(market) = rows.get(index).cloned() else {
                            return div().into_any_element();
                        };
                        let active = market.symbol == selected;
                        entity
                            .upgrade()
                            .map(|entity| {
                                entity.update(cx, |this, cx| {
                                    this.market_row(market, active, theme, cx)
                                })
                            })
                            .unwrap_or_else(|| div().into_any_element())
                    })
                    .size_full(),
                ),
            )
    }

    fn market_row(
        &self,
        market: MarketListing,
        active: bool,
        theme: Theme,
        cx: &mut Context<Self>,
    ) -> gpui::AnyElement {
        let symbol = market.symbol.clone();
        let focus = cx.focus_handle();
        div()
            .id(SharedString::from(format!("market-{}", market.symbol)))
            .track_focus(&focus)
            .tab_index(0)
            .tab_stop(true)
            .h(px(MARKET_ROW_HEIGHT))
            .px(px(10.0))
            .flex()
            .items_center()
            .justify_between()
            .cursor_default()
            .bg(if active {
                theme.sidebar_item_background
            } else {
                gpui::transparent_black()
            })
            .hover(|style| style.bg(theme.overlay))
            .focus_visible(|style| style.border_1().border_color(theme.accent))
            .on_activation(cx, move |this, _, cx| {
                this.select_symbol(symbol.clone(), cx);
            })
            .child(
                div()
                    .text_size(px(12.0))
                    .font_weight(if active {
                        FontWeight::SEMIBOLD
                    } else {
                        FontWeight::NORMAL
                    })
                    .child(SharedString::from(market.symbol)),
            )
            .child(
                div()
                    .text_size(px(10.0))
                    .text_color(theme.text_tertiary)
                    .child(SharedString::from(if market.max_leverage > 0.0 {
                        format!("{}x", market.max_leverage as i64)
                    } else {
                        "—".into()
                    })),
            )
            .into_any_element()
    }

    fn render_chart_and_tables(
        &self,
        _snapshot: Option<&DeskSnapshot>,
        candles: &Arc<Vec<proof_phoenix::Candle>>,
        timeframe: Timeframe,
        theme: Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .flex_1()
            .min_w(px(0.0))
            .h_full()
            .flex()
            .flex_col()
            .child(self.render_timeframes(timeframe, theme, cx))
            .child(self.render_chart(candles, theme, cx))
            .child(self.render_tables(theme))
    }

    fn render_timeframes(
        &self,
        selected: Timeframe,
        theme: Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let mut row = div()
            .h(px(32.0))
            .flex_none()
            .px(px(10.0))
            .flex()
            .items_center()
            .gap(px(6.0))
            .border_b_1()
            .border_color(theme.border);
        for timeframe in Timeframe::ALL {
            let active = timeframe == selected;
            let focus = cx.focus_handle();
            row = row.child(
                div()
                    .id(SharedString::from(format!("tf-{}", timeframe.as_api())))
                    .track_focus(&focus)
                    .tab_index(0)
                    .tab_stop(true)
                    .h(px(22.0))
                    .px(px(8.0))
                    .rounded(px(5.0))
                    .flex()
                    .items_center()
                    .cursor_default()
                    .bg(if active {
                        theme.overlay_strong
                    } else {
                        theme.overlay
                    })
                    .text_size(px(11.0))
                    .text_color(if active {
                        theme.text
                    } else {
                        theme.text_tertiary
                    })
                    .focus_visible(|style| style.border_1().border_color(theme.accent))
                    .on_activation(cx, move |this, _, cx| this.set_timeframe(timeframe, cx))
                    .child(timeframe.label()),
            );
        }
        row
    }

    fn render_chart(
        &self,
        candles: &Arc<Vec<proof_phoenix::Candle>>,
        theme: Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let candles = candles.clone();
        let visible = self.chart_visible.max(20);
        let offset = self.chart_offset.min(candles.len().saturating_sub(visible));
        let windowed = window_candles(&candles, visible, offset);
        let up = theme.success;
        let down = theme.danger;
        let grid = theme.border;
        let focus = cx.focus_handle();
        div()
            .id("proof-chart")
            .track_focus(&focus)
            .tab_index(0)
            .tab_stop(true)
            .flex_1()
            .min_h(px(180.0))
            .p(px(8.0))
            .cursor(if self.chart_drag.is_some() {
                CursorStyle::ClosedHand
            } else {
                CursorStyle::OpenHand
            })
            .focus_visible(|style| style.border_1().border_color(theme.accent))
            .on_scroll_wheel(cx.listener(Self::on_chart_scroll))
            .on_pinch(cx.listener(Self::on_chart_pinch))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, event: &MouseDownEvent, _, cx| {
                    this.chart_drag = Some((f32::from(event.position.x), this.chart_offset));
                    cx.stop_propagation();
                    cx.notify();
                }),
            )
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(|this, _: &MouseUpEvent, _, cx| this.end_chart_drag(cx)),
            )
            .on_mouse_up_out(
                MouseButton::Left,
                cx.listener(|this, _: &MouseUpEvent, _, cx| this.end_chart_drag(cx)),
            )
            .on_click(cx.listener(|this, event: &ClickEvent, _, cx| {
                if event.click_count() >= 2 {
                    this.reset_chart(cx);
                    cx.stop_propagation();
                }
            }))
            .child(
                canvas(
                    |_, _, _| (),
                    move |bounds, _, window, _| {
                        paint_candles(bounds, &windowed, up, down, grid, window);
                    },
                )
                .size_full(),
            )
    }

    fn on_chart_scroll(
        &mut self,
        event: &ScrollWheelEvent,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let pixel_delta = match event.delta {
            ScrollDelta::Pixels(delta) => (f32::from(delta.x), f32::from(delta.y)),
            ScrollDelta::Lines(delta) => (delta.x * 24.0, delta.y * 24.0),
        };
        if event.modifiers.control || event.modifiers.alt {
            if pixel_delta.1.abs() < 0.1 {
                return;
            }
            self.zoom_chart(pixel_delta.1 > 0.0, cx);
        } else if pixel_delta.0.abs() > pixel_delta.1.abs() {
            if pixel_delta.0.abs() < 0.1 {
                return;
            }
            let bars = (pixel_delta.0 / 8.0).round() as isize;
            if bars != 0 {
                self.pan_chart(bars, cx);
            }
        } else {
            if pixel_delta.1.abs() < 0.1 {
                return;
            }
            self.zoom_chart(pixel_delta.1 > 0.0, cx);
        }
        cx.stop_propagation();
    }

    fn on_chart_pinch(&mut self, event: &PinchEvent, _: &mut Window, cx: &mut Context<Self>) {
        if event.delta.abs() < 0.01 {
            return;
        }
        self.zoom_chart(event.delta > 0.0, cx);
        cx.stop_propagation();
    }

    fn on_chart_drag(&mut self, event: &MouseMoveEvent, _: &mut Window, cx: &mut Context<Self>) {
        let Some((origin_x, origin_offset)) = self.chart_drag else {
            return;
        };
        let len = self.candles.len();
        let visible = self.chart_visible.clamp(20, len.max(20));
        let slot = 8.0_f32;
        let dx = f32::from(event.position.x) - origin_x;
        let shift = (dx / slot).round() as isize;
        let max_offset = len.saturating_sub(visible) as isize;
        let next = (origin_offset as isize - shift).clamp(0, max_offset) as usize;
        if next == self.chart_offset {
            return;
        }
        self.chart_offset = next;
        cx.stop_propagation();
        cx.notify();
    }

    fn render_tables(&self, theme: Theme) -> impl IntoElement {
        div()
            .h(px(168.0))
            .flex_none()
            .flex()
            .border_t_1()
            .border_color(theme.border)
            .child(locked_table(
                "Positions",
                "Connect a wallet to view positions",
                theme,
            ))
            .child(locked_table(
                "Open orders",
                "No resting orders — live trading is later",
                theme,
            ))
    }

    fn render_book_and_ticket(
        &self,
        snapshot: Option<&DeskSnapshot>,
        theme: Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        div()
            .w(px(320.0))
            .h_full()
            .flex_none()
            .flex()
            .flex_col()
            .border_l_1()
            .border_color(theme.border)
            .child(self.render_book(snapshot, theme, cx))
            .child(self.render_ticket(snapshot, theme, cx))
            .child(self.render_trades(snapshot, theme, cx))
    }

    fn render_book(
        &self,
        snapshot: Option<&DeskSnapshot>,
        theme: Theme,
        _cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let asks = snapshot
            .map(|desk| desk.book.asks[..desk.book.asks.len().min(BOOK_DEPTH)].to_vec())
            .unwrap_or_default();
        let bids = snapshot
            .map(|desk| desk.book.bids[..desk.book.bids.len().min(BOOK_DEPTH)].to_vec())
            .unwrap_or_default();
        let mid = snapshot.and_then(|desk| desk.mark.or(desk.book.mid));
        let ask_rows = Arc::new(asks);
        let bid_rows = Arc::new(bids);
        let ask_max = ask_rows
            .iter()
            .map(|level| level.size)
            .fold(0.0, f64::max)
            .max(1.0);
        let bid_max = bid_rows
            .iter()
            .map(|level| level.size)
            .fold(0.0, f64::max)
            .max(1.0);

        div()
            .flex_1()
            .min_h(px(160.0))
            .flex()
            .flex_col()
            .child(section_label("Order book", theme))
            .child(book_header(theme))
            .child(div().flex_1().min_h(px(0.0)).child({
                let rows = ask_rows.clone();
                list(self.book_ask_list.clone(), move |index, _, _| {
                    rows.get(index)
                        .cloned()
                        .map(|level| book_row(level, false, ask_max, theme))
                        .unwrap_or_else(|| div().into_any_element())
                })
                .size_full()
            }))
            .child(
                div()
                    .h(px(28.0))
                    .flex_none()
                    .px(px(10.0))
                    .flex()
                    .items_center()
                    .justify_center()
                    .bg(theme.overlay)
                    .child(
                        div()
                            .text_size(px(13.0))
                            .font_weight(FontWeight::SEMIBOLD)
                            .child(SharedString::from(
                                mid.map(format::price).unwrap_or_else(|| "—".into()),
                            )),
                    ),
            )
            .child(div().flex_1().min_h(px(0.0)).child({
                let rows = bid_rows.clone();
                list(self.book_bid_list.clone(), move |index, _, _| {
                    rows.get(index)
                        .cloned()
                        .map(|level| book_row(level, true, bid_max, theme))
                        .unwrap_or_else(|| div().into_any_element())
                })
                .size_full()
            }))
    }

    fn render_ticket(
        &self,
        snapshot: Option<&DeskSnapshot>,
        theme: Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let mark = snapshot.and_then(|desk| desk.mark);
        let long = self.ticket_side_long;
        let limit = self.ticket_is_limit;
        div()
            .h(px(214.0))
            .flex_none()
            .flex()
            .flex_col()
            .gap(px(8.0))
            .p(px(10.0))
            .border_t_1()
            .border_color(theme.border)
            .child(section_label("Order ticket", theme).pl(px(0.0)))
            .child(
                div()
                    .flex()
                    .gap(px(6.0))
                    .child(self.mode_chip("Cross", true, theme, cx, |_| {}))
                    .child(self.mode_chip("Isolated", false, theme, cx, |_| {})),
            )
            .child(
                div()
                    .flex()
                    .gap(px(6.0))
                    .child(self.mode_chip("Limit", limit, theme, cx, |this| {
                        this.ticket_is_limit = true;
                    }))
                    .child(self.mode_chip("Market", !limit, theme, cx, |this| {
                        this.ticket_is_limit = false;
                    })),
            )
            .child(
                div()
                    .flex()
                    .gap(px(6.0))
                    .child(self.side_chip("Long / Buy", true, long, theme, cx))
                    .child(self.side_chip("Short / Sell", false, !long, theme, cx)),
            )
            .child(locked_field("Size", "0.00 SOL", theme))
            .child(locked_field(
                "Limit price",
                mark.map(format::price).unwrap_or_else(|| "—".into()),
                theme,
            ))
            .child(
                div()
                    .h(px(28.0))
                    .rounded(px(6.0))
                    .bg(theme.overlay)
                    .flex()
                    .items_center()
                    .justify_center()
                    .text_size(px(11.5))
                    .text_color(theme.text_tertiary)
                    .child("Locked — connect wallet in P1"),
            )
    }

    fn mode_chip(
        &self,
        label: &'static str,
        active: bool,
        theme: Theme,
        cx: &mut Context<Self>,
        on_select: impl Fn(&mut Self) + 'static,
    ) -> impl IntoElement {
        let focus = cx.focus_handle();
        div()
            .id(label)
            .track_focus(&focus)
            .tab_index(0)
            .tab_stop(true)
            .h(px(22.0))
            .px(px(8.0))
            .rounded(px(5.0))
            .flex()
            .items_center()
            .cursor_default()
            .bg(if active {
                theme.overlay_strong
            } else {
                theme.overlay
            })
            .text_size(px(11.0))
            .focus_visible(|style| style.border_1().border_color(theme.accent))
            .on_activation(cx, move |this, _, cx| {
                on_select(this);
                cx.notify();
            })
            .child(label)
    }

    fn side_chip(
        &self,
        label: &'static str,
        long: bool,
        active: bool,
        theme: Theme,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let focus = cx.focus_handle();
        let color = if long { theme.success } else { theme.danger };
        div()
            .id(label)
            .track_focus(&focus)
            .tab_index(0)
            .tab_stop(true)
            .flex_1()
            .h(px(26.0))
            .rounded(px(6.0))
            .flex()
            .items_center()
            .justify_center()
            .cursor_default()
            .bg(if active {
                color.opacity(0.18)
            } else {
                theme.overlay
            })
            .text_color(if active { color } else { theme.text_secondary })
            .text_size(px(11.5))
            .focus_visible(|style| style.border_1().border_color(theme.accent))
            .on_activation(cx, move |this, _, cx| {
                this.ticket_side_long = long;
                cx.notify();
            })
            .child(label)
    }

    fn render_trades(
        &self,
        snapshot: Option<&DeskSnapshot>,
        theme: Theme,
        _cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let trades = snapshot.map(|desk| desk.trades.clone()).unwrap_or_default();
        let rows = Arc::new(trades);
        div()
            .h(px(148.0))
            .flex_none()
            .flex()
            .flex_col()
            .border_t_1()
            .border_color(theme.border)
            .child(section_label("Trades", theme))
            .child(
                div().flex_1().min_h(px(0.0)).child(
                    list(self.trade_list.clone(), move |index, _, _| {
                        rows.get(index)
                            .cloned()
                            .map(|trade| trade_row(trade, theme))
                            .unwrap_or_else(|| div().into_any_element())
                    })
                    .size_full(),
                ),
            )
    }

    fn render_bottom(
        &self,
        snapshot: Option<&DeskSnapshot>,
        error: Option<&str>,
        theme: Theme,
    ) -> impl IntoElement {
        let health = "Health  —  wallet later";
        let status = snapshot
            .map(|desk| desk.status.clone())
            .unwrap_or_else(|| "loading".into());
        let detail = error
            .map(|error| error.to_owned())
            .unwrap_or_else(|| format!("Phoenix {status} · paper mode · no signing"));
        div()
            .h(px(28.0))
            .flex_none()
            .px(px(12.0))
            .flex()
            .items_center()
            .justify_between()
            .border_t_1()
            .border_color(theme.border)
            .bg(theme.surface)
            .child(
                div()
                    .text_size(px(11.0))
                    .text_color(theme.text_tertiary)
                    .child(health),
            )
            .child(
                div()
                    .text_size(px(11.0))
                    .text_color(if error.is_some() {
                        theme.warning
                    } else {
                        theme.text_tertiary
                    })
                    .child(SharedString::from(detail)),
            )
    }
}

fn section_label(label: &'static str, theme: Theme) -> gpui::Stateful<gpui::Div> {
    div()
        .id(label)
        .h(px(28.0))
        .px(px(10.0))
        .flex()
        .items_center()
        .text_size(px(11.0))
        .text_color(theme.text_tertiary)
        .child(label)
}

fn stat_chip(
    label: &'static str,
    value: String,
    color: gpui::Hsla,
    theme: Theme,
) -> impl IntoElement {
    div()
        .flex()
        .items_baseline()
        .gap(px(5.0))
        .child(
            div()
                .text_size(px(10.0))
                .text_color(theme.text_ghost)
                .child(label),
        )
        .child(
            div()
                .text_size(px(12.5))
                .font_weight(FontWeight::MEDIUM)
                .text_color(color)
                .child(SharedString::from(value)),
        )
}

fn book_header(theme: Theme) -> impl IntoElement {
    div()
        .h(px(18.0))
        .px(px(10.0))
        .flex()
        .text_size(px(10.0))
        .text_color(theme.text_ghost)
        .child(div().flex_1().child("Price"))
        .child(div().w(px(72.0)).child("Size"))
}

fn book_row(
    level: proof_phoenix::BookLevel,
    bid: bool,
    max_size: f64,
    theme: Theme,
) -> gpui::AnyElement {
    let color = if bid { theme.success } else { theme.danger };
    let fill_frac = (level.size / max_size).clamp(0.0, 1.0) as f32;
    div()
        .h(px(BOOK_ROW_HEIGHT))
        .px(px(10.0))
        .relative()
        .flex()
        .items_center()
        .child(
            div()
                .absolute()
                .top_0()
                .bottom_0()
                .right_0()
                .w(relative(fill_frac))
                .bg(color.opacity(0.12)),
        )
        .child(
            div()
                .flex_1()
                .text_size(px(11.0))
                .text_color(color)
                .child(SharedString::from(format::price(level.price))),
        )
        .child(
            div()
                .w(px(72.0))
                .text_size(px(11.0))
                .text_color(theme.text_secondary)
                .child(SharedString::from(format::size(level.size))),
        )
        .into_any_element()
}

fn trade_row(trade: proof_phoenix::Trade, theme: Theme) -> gpui::AnyElement {
    let color = if trade.is_buy {
        theme.success
    } else {
        theme.danger
    };
    div()
        .h(px(TRADE_ROW_HEIGHT))
        .px(px(10.0))
        .flex()
        .items_center()
        .child(
            div()
                .flex_1()
                .text_size(px(11.0))
                .text_color(color)
                .child(SharedString::from(format::price(trade.price))),
        )
        .child(
            div()
                .w(px(64.0))
                .text_size(px(11.0))
                .text_color(theme.text_secondary)
                .child(SharedString::from(format::size(trade.size))),
        )
        .child(
            div()
                .w(px(54.0))
                .text_size(px(10.0))
                .text_color(theme.text_ghost)
                .child(SharedString::from(format::clock_label(&trade.timestamp))),
        )
        .into_any_element()
}

fn locked_table(title: &'static str, empty: &'static str, theme: Theme) -> impl IntoElement {
    div()
        .flex_1()
        .h_full()
        .flex()
        .flex_col()
        .border_r_1()
        .border_color(theme.border)
        .child(section_label(title, theme))
        .child(
            div()
                .flex_1()
                .px(px(12.0))
                .flex()
                .items_center()
                .text_size(px(12.0))
                .text_color(theme.text_ghost)
                .child(empty),
        )
}

fn locked_field(label: &'static str, value: impl Into<String>, theme: Theme) -> impl IntoElement {
    div()
        .flex()
        .items_center()
        .justify_between()
        .child(
            div()
                .text_size(px(11.0))
                .text_color(theme.text_ghost)
                .child(label),
        )
        .child(
            div()
                .h(px(24.0))
                .px(px(8.0))
                .min_w(px(92.0))
                .rounded(px(5.0))
                .bg(theme.inset)
                .flex()
                .items_center()
                .justify_end()
                .text_size(px(11.5))
                .text_color(theme.text_secondary)
                .child(SharedString::from(value.into())),
        )
}

fn set_list_len(list: &ListState, len: usize) {
    if list.item_count() != len {
        list.reset(len);
    }
}

fn paint_candles(
    bounds: Bounds<Pixels>,
    candles: &[proof_phoenix::Candle],
    up: gpui::Hsla,
    down: gpui::Hsla,
    grid: gpui::Hsla,
    window: &mut Window,
) {
    window.paint_quad(fill(bounds, gpui::transparent_black()));
    if candles.len() < 2 {
        return;
    }
    let width = f32::from(bounds.size.width);
    let height = f32::from(bounds.size.height);
    if width <= 1.0 || height <= 1.0 {
        return;
    }

    let mut min = f64::MAX;
    let mut max = f64::MIN;
    for candle in candles {
        min = min.min(candle.low);
        max = max.max(candle.high);
    }
    if !min.is_finite() || !max.is_finite() || (max - min).abs() < f64::EPSILON {
        max = min + 1.0;
    }
    let pad = (max - min) * 0.08;
    min -= pad;
    max += pad;
    let range = max - min;

    for fraction in [0.25, 0.5, 0.75] {
        let y = bounds.origin.y + px(height * fraction);
        window.paint_quad(fill(
            Bounds::new(point(bounds.origin.x, y), size(bounds.size.width, px(1.0))),
            grid,
        ));
    }

    let slot = width / candles.len() as f32;
    let body_w = (slot * 0.62).clamp(1.0, 7.0);
    for (index, candle) in candles.iter().enumerate() {
        let x = f32::from(bounds.origin.x) + index as f32 * slot + slot / 2.0;
        let y_of = |price: f64| {
            let t = ((max - price) / range) as f32;
            f32::from(bounds.origin.y) + t * height
        };
        let high = y_of(candle.high);
        let low = y_of(candle.low);
        let open = y_of(candle.open);
        let close = y_of(candle.close);
        let color = if candle.close >= candle.open {
            up
        } else {
            down
        };
        let wick_top = high.min(low);
        let wick_h = (high - low).abs().max(1.0);
        window.paint_quad(fill(
            Bounds::new(point(px(x), px(wick_top)), size(px(1.0), px(wick_h))),
            color,
        ));
        let body_top = open.min(close);
        let body_h = (open - close).abs().max(1.0);
        window.paint_quad(fill(
            Bounds::new(
                point(px(x - body_w / 2.0), px(body_top)),
                size(px(body_w), px(body_h)),
            ),
            color,
        ));
    }
}

impl ProofDesk {
    fn titlebar_drag(
        &self,
        region: Stateful<gpui::Div>,
        cx: &mut Context<Self>,
    ) -> Stateful<gpui::Div> {
        #[cfg(target_os = "windows")]
        let region = region.window_control_area(gpui::WindowControlArea::Drag);
        region
            .on_click(|event, window, _| {
                if event.click_count() == 2 {
                    crate::platform::titlebar_double_click(window);
                }
            })
            .on_mouse_down_out(cx.listener(|this, _, _, _| {
                this.header_drag_armed = false;
            }))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, _| {
                    this.header_drag_armed = true;
                }),
            )
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(|this, _, _, _| {
                    this.header_drag_armed = false;
                }),
            )
            .on_mouse_move(cx.listener(|this, _, window, _| {
                if this.header_drag_armed {
                    this.header_drag_armed = false;
                    crate::platform::start_window_move(window);
                }
            }))
    }
}
