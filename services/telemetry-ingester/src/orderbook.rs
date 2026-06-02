use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};

use ordered_float::OrderedFloat;

use crate::types::{ExpectedFill, Order};

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: BTreeMap<Reverse<OrderedFloat<f64>>, Vec<Order>>,
    pub asks: BTreeMap<OrderedFloat<f64>, Vec<Order>>,
    pub orders: HashMap<String, Order>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: HashMap::new(),
        }
    }

    pub fn add_order(&mut self, order: Order) {
        let price = OrderedFloat::from(order.price);
        let order_id = order.order_id.clone();
        let cloned = order.clone();

        match order.side {
            1 => {
                self.bids.entry(Reverse(price)).or_default().push(order);
            }
            2 => {
                self.asks.entry(price).or_default().push(order);
            }
            _ => return,
        }

        self.orders.insert(order_id, cloned);
    }

    pub fn cancel_order(&mut self, order_id: &str) -> Option<Order> {
        let order = self.orders.remove(order_id)?;

        match order.side {
            1 => {
                let price = Reverse(OrderedFloat::from(order.price));
                if let Some(orders) = self.bids.get_mut(&price) {
                    orders.retain(|o| o.order_id != order_id);
                    if orders.is_empty() {
                        self.bids.remove(&price);
                    }
                }
            }
            2 => {
                let price = OrderedFloat::from(order.price);
                if let Some(orders) = self.asks.get_mut(&price) {
                    orders.retain(|o| o.order_id != order_id);
                    if orders.is_empty() {
                        self.asks.remove(&price);
                    }
                }
            }
            _ => {}
        }

        Some(order)
    }

    /// Computes what fills SHOULD happen before adding the order to the book.
    /// For buy orders: walks asks from best (lowest) price until filled.
    /// For sell orders: walks bids from best (highest) price until filled.
    pub fn expected_fills(&self, order: &Order) -> Vec<ExpectedFill> {
        let mut fills = Vec::new();
        let mut remaining = order.qty;

        match order.side {
            1 => {
                for (ask_price, orders) in self.asks.iter() {
                    if order.ord_type == 2 && **ask_price > order.price {
                        break;
                    }
                    for resting in orders {
                        if remaining == 0 {
                            break;
                        }
                        let qty = remaining.min(resting.qty);
                        fills.push(ExpectedFill {
                            resting_order_id: resting.order_id.clone(),
                            fill_price: resting.price,
                            fill_qty: qty,
                        });
                        remaining -= qty;
                    }
                    if remaining == 0 {
                        break;
                    }
                }
            }
            2 => {
                for (&Reverse(bid_price), orders) in self.bids.iter() {
                    if order.ord_type == 2 && *bid_price < order.price {
                        break;
                    }
                    for resting in orders {
                        if remaining == 0 {
                            break;
                        }
                        let qty = remaining.min(resting.qty);
                        fills.push(ExpectedFill {
                            resting_order_id: resting.order_id.clone(),
                            fill_price: resting.price,
                            fill_qty: qty,
                        });
                        remaining -= qty;
                    }
                    if remaining == 0 {
                        break;
                    }
                }
            }
            _ => {}
        }

        fills
    }

    /// Adds the order to the book (call AFTER expected_fills).
    /// Only limit orders are added to the book; market orders trade immediately.
    pub fn update_from_order(&mut self, order: &Order) {
        if order.ord_type != 2 {
            return;
        }
        self.add_order(order.clone());
    }
}
