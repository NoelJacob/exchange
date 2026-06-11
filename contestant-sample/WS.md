# XCANG3 Exchange — WS 2.0 Protocol Specification (Strict JSON-RPC 2.0)

This document defines the JSON-RPC 2.0 WebSocket protocol for the XCANG3 Exchange. Message structures are documented with concrete examples and formal JSON Schema specifications to enable direct integration with standard JSON-RPC client/server libraries and schema validators.

## 1. Protocol Mechanics & Envelope Standards

### 1.1 Client-Initiated Request Envelope

All client requests must contain standard JSON-RPC 2.0 members. The presence of the id member indicates a synchronous request-response call.

#### Client Request Example

```json
{
  "jsonrpc": "2.0",
  "method": "order.create.limit",
  "params": {},
  "id": 10003
}
```

#### Client Request Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "method": { "type": "string" },
    "params": { "type": "object" },
    "id": { "type": "integer" }
  },
  "required": ["jsonrpc", "method", "params", "id"]
}
```

### 1.2 Server Synchronous Response Envelope (Added/Fill/Partial/Reject Success)

Returned immediately by the server upon accepting a valid message into the gateway queue. It contains only jsonrpc, result, and matching id. For Market Order we have Fill/Partial/Reject but for Limit Order we have Added/Fill/Partial/Reject.

#### Success Response Example

```json
{
  "jsonrpc": "2.0",
  "result": {},
  "id": 10003
}
```

#### Success Response Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "result": { "type": "object" },
    "id": { "type": "integer" }
  },
  "required": ["jsonrpc", "result", "id"]
}
```

### 1.3 Server Synchronous Response Envelope (Parameter Error)

Returned immediately if a client request fails basic formatting, transport validation, or parameter constraints.

#### Error Response Example

```json
{
  "jsonrpc": "2.0",
  "error": {
    "code": -32602,
    "message": "Invalid params",
    "data": {
      "details": "Missing parameter: 'price' is required for limit orders."
    }
  },
  "id": 10003
}
```

#### Error Response Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "error": {
      "type": "object",
      "properties": {
        "code": { "type": "integer" },
        "message": { "type": "string" },
        "data": {
          "type": "object",
          "properties": {
            "details": { "type": "string" }
          },
          "required": ["details"]
        }
      },
      "required": ["code", "message"]
    },
    "id": { "type": "integer" }
  },
  "required": ["jsonrpc", "error", "id"]
}
```

### 1.4 Server-Initiated Async Notification Envelope

Engine updates, execution reports, and cancel updates are pushed as server-initiated notifications. They do not contain a server-generated id.

#### Server-Initiated Notification Example

```json
{
  "jsonrpc": "2.0",
  "method": "order.report.fill",
  "params": {}
}
```

#### Server-Initiated Notification Schema

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "method": { "type": "string" },
    "params": { "type": "object" }
  },
  "required": ["jsonrpc", "method", "params"]
}
```

## 2. Client-Initiated Transactions

### 2.1 Limit Order Placement (order.create.limit)

#### CLIENT Request

```json
{
  "jsonrpc": "2.0",
  "method": "order.create.limit",
  "params": {
    "sender_id": "CLIENT01",
    "target_id": "XCANG3",
    "sending_time": "2026-06-04T14:34:00.000Z",
    "cl_ord_id": "CL_ORD_03",
    "symbol": "AAPL",
    "side": "buy",
    "qty": 100,
    "price": 150.25
  },
  "id": 10003
}
```

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "method": { "type": "string", "const": "order.create.limit" },
    "params": {
      "type": "object",
      "properties": {
        "sender_id": { "type": "string" },
        "target_id": { "type": "string", "const": "XCANG3" },
        "sending_time": { "type": "string", "format": "date-time" },
        "cl_ord_id": { "type": "string" },
        "symbol": { "type": "string" },
        "side": { "type": "string", "enum": ["buy", "sell"] },
        "qty": { "type": "integer", "minimum": 1 },
        "price": { "type": "number", "minimum": 0.0001 }
      },
      "required": [
        "sender_id",
        "target_id",
        "sending_time",
        "cl_ord_id",
        "symbol",
        "side",
        "qty",
        "price"
      ]
    },
    "id": { "type": "integer" }
  },
  "required": ["jsonrpc", "method", "params", "id"]
}
```

#### XCANG3 Synchronous Response (New)

```json
{
  "jsonrpc": "2.0",
  "result": {
    "method": "order.report.new",
    "params": {
      "sender_id": "XCANG3",
      "target_id": "CLIENT01",
      "transact_time": "2026-06-04T14:35:05.000Z",
      "cl_ord_id": "CL_ORD_03",
      "ex_ord_id": "EX_ORD_99",
      "exec_id": "FILL_883",
      "symbol": "AAPL",
      "side": "buy",
      "qty": 100,
      "price": 150.25
    }
  },
  "id": 10003
}
```

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "result": {
      "type": "object",
      "properties": {
        "method": { "type": "string", "const": "order.report.new" },
        "params": {
          "type": "object",
          "properties": {
            "sender_id": { "type": "string", "const": "XCANG3" },
            "target_id": { "type": "string" },
            "transact_time": { "type": "string", "format": "date-time" },
            "cl_ord_id": { "type": "string" },
            "ex_ord_id": { "type": "string" },
            "exec_id": { "type": "string" },
            "symbol": { "type": "string" },
            "side": { "type": "string", "enum": ["buy", "sell"] },
            "qty": { "type": "integer" },
            "price": { "type": "number", "minimum": 0.0001 },
          },
          "required": [
            "sender_id",
            "target_id",
            "ex_ord_id",
            "exec_id",
            "symbol",
            "side",
            "qty",
            "cl_ord_id",
            "transact_time",
            "price",
          ]
        }
      },
      "required": ["method", "params"]
    },
    "id": { "type": "integer" }
  },
  "required": ["jsonrpc", "result", "id"]
}
```

#### XCANG3 Synchronous Response (Fill)

```json
{
  "jsonrpc": "2.0",
  "result": {
    "method": "order.report.fill",
    "params": {
      "sender_id": "XCANG3",
      "target_id": "CLIENT01",
      "transact_time": "2026-06-04T14:35:05.000Z",
      "cl_ord_id": "CL_ORD_03",
      "ex_ord_id": "EX_ORD_99",
      "exec_id": "FILL_883",
      "symbol": "AAPL",
      "side": "buy",
      "qty": 100,
      "leaves_qty": 0,
      "cum_qty": 100,
      "last_shares": 60,
      "last_px": 150.25,
      "avg_px": 150.25,
      "price": 150.25,
    }
  },
  "id": 10003
}
```

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "result": {
      "type": "object",
      "properties": {
        "method": { "type": "string", "const": "order.report.fill" },
        "params": {
          "type": "object",
          "properties": {
            "sender_id": { "type": "string", "const": "XCANG3" },
            "target_id": { "type": "string" },
            "transact_time": { "type": "string", "format": "date-time" },
            "cl_ord_id": { "type": "string" },
            "ex_ord_id": { "type": "string" },
            "exec_id": { "type": "string" },
            "symbol": { "type": "string" },
            "side": { "type": "string", "enum": ["buy", "sell"] },
            "qty": { "type": "integer" },
            "leaves_qty": { "type": "integer", "const": 0 },
            "cum_qty": { "type": "integer" },
            "last_shares": { "type": "integer" },
            "last_px": { "type": "number" },
            "avg_px": { "type": "number" },
            "price": { "type": "number", "minimum": 0.0001 },
          },
          "required": [
            "sender_id",
            "target_id",
            "transact_time",
            "cl_ord_id",
            "ex_ord_id",
            "exec_id",
            "symbol",
            "side",
            "qty",
            "leaves_qty",
            "cum_qty",
            "last_shares",
            "last_px",
            "avg_px",
            "price",
          ]
        }
      },
      "required": ["method", "params"]
    },
    "id": { "type": "integer" }
  },
  "required": ["jsonrpc", "result", "id"]
}
```

#### XCANG3 Synchronous Response (Partial)

```json
{
  "jsonrpc": "2.0",
  "result": {
    "method": "order.report.partial",
    "params": {
      "sender_id": "XCANG3",
      "target_id": "CLIENT01",
      "transact_time": "2026-06-04T14:35:00.000Z",
      "cl_ord_id": "CL_ORD_03",
      "ex_ord_id": "EX_ORD_99",
      "exec_id": "FILL_882",
      "symbol": "AAPL",
      "side": "buy",
      "qty": 100,
      "leaves_qty": 60,
      "cum_qty": 40,
      "last_shares": 40,
      "last_px": 150.25,
      "avg_px": 150.25,
      "price": 150.25,
    }
  },
  "id": 10003
}
```

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "result": {
      "type": "object",
      "properties": {
        "method": { "type": "string", "const": "order.report.partial" },
        "params": {
          "type": "object",
          "properties": {
            "sender_id": { "type": "string", "const": "XCANG3" },
            "target_id": { "type": "string" },
            "transact_time": { "type": "string", "format": "date-time" },
            "cl_ord_id": { "type": "string" },
            "ex_ord_id": { "type": "string" },
            "exec_id": { "type": "string" },
            "symbol": { "type": "string" },
            "side": { "type": "string", "enum": ["buy", "sell"] },
            "qty": { "type": "integer" },
            "leaves_qty": { "type": "integer", "minimum": 1 },
            "cum_qty": { "type": "integer" },
            "last_shares": { "type": "integer" },
            "last_px": { "type": "number" },
            "avg_px": { "type": "number" },
            "price": { "type": "number", "minimum": 0.0001 },
          },
          "required": [
            "sender_id",
            "target_id",
            "transact_time",
            "cl_ord_id",
            "ex_ord_id",
            "exec_id",
            "symbol",
            "side",
            "qty",
            "leaves_qty",
            "cum_qty",
            "last_shares",
            "last_px",
            "avg_px",
            "price",
          ]
        }
      },
      "required": ["method", "params"]
    },
    "id": { "type": "integer" }
  },
  "required": ["jsonrpc", "result", "id"]
}
```

#### XCANG3 Synchronous Response (Business Logic Order Reject)

```json
{
  "jsonrpc": "2.0",
  "result": {
    "method": "order.report.rejected",
    "params": {
      "sender_id": "XCANG3",
      "target_id": "CLIENT01",
      "transact_time": "2026-06-04T14:33:06.000Z",
      "cl_ord_id": "CL_ORD_03",
      "ex_ord_id": "NONE",
      "exec_id": "REJ_771",
      "symbol": "AAPL",
      "side": "sell",
      "qty": 100,
      "reject_reason": "Insufficient margin balance",
      "price": 150.25,
    }
  },
  "id": 10003
}
```

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "result": {
      "type": "object",
      "properties": {
        "method": { "type": "string", "const": "order.report.rejected" },
        "params": {
          "type": "object",
          "properties": {
            "sender_id": { "type": "string", "const": "XCANG3" },
            "target_id": { "type": "string" },
            "transact_time": { "type": "string", "format": "date-time" },
            "cl_ord_id": { "type": "string" },
            "ex_ord_id": { "type": "string" },
            "exec_id": { "type": "string" },
            "symbol": { "type": "string" },
            "side": { "type": "string", "enum": ["buy", "sell"] },
            "qty": { "type": "integer" },
            "reject_reason": { "type": "string" },
            "price": { "type": "number", "minimum": 0.0001 }
          },
          "required": [
            "sender_id",
            "target_id",
            "transact_time",
            "cl_ord_id",
            "ex_ord_id",
            "exec_id",
            "symbol",
            "side",
            "qty",
            "reject_reason",
            "price",
          ]
        }
      },
      "required": ["method", "params"]
    },
    "id": { "type": "integer" }
  },
  "required": ["jsonrpc", "result", "id"]
}
```

### 2.2 Market Order Placement (order.create.market)
They only have filled or rejected responses.

#### CLIENT Request

```json
{
  "jsonrpc": "2.0",
  "method": "order.create.market",
  "params": {
    "sender_id": "CLIENT01",
    "target_id": "XCANG3",
    "sending_time": "2026-06-04T14:33:05.000Z",
    "cl_ord_id": "CL_ORD_03",
    "symbol": "AAPL",
    "side": "sell",
    "qty": 100
  },
  "id": 10003
}
```

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "method": { "type": "string", "const": "order.create.market" },
    "params": {
      "type": "object",
      "properties": {
        "sender_id": { "type": "string" },
        "target_id": { "type": "string", "const": "XCANG3" },
        "sending_time": { "type": "string", "format": "date-time" },
        "cl_ord_id": { "type": "string" },
        "symbol": { "type": "string" },
        "side": { "type": "string", "enum": ["buy", "sell"] },
        "qty": { "type": "integer", "minimum": 1 }
      },
      "required": [
        "sender_id",
        "target_id",
        "sending_time",
        "cl_ord_id",
        "symbol",
        "side",
        "qty"
      ]
    },
    "id": { "type": "integer" }
  },
  "required": ["jsonrpc", "method", "params", "id"]
}
```

#### XCANG3 Synchronous Response (Fill)

```json
{
  "jsonrpc": "2.0",
  "result": {
    "method": "order.report.fill",
    "params": {
      "sender_id": "XCANG3",
      "target_id": "CLIENT01",
      "transact_time": "2026-06-04T14:35:05.000Z",
      "cl_ord_id": "CL_ORD_03",
      "ex_ord_id": "EX_ORD_99",
      "exec_id": "FILL_883",
      "symbol": "AAPL",
      "side": "sell",
      "qty": 100,
      "leaves_qty": 0,
      "cum_qty": 100,
      "last_shares": 100,
      "last_px": 150.25,
      "avg_px": 150.25
    }
  },
  "id": 10003
}
```

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "result": {
      "type": "object",
      "properties": {
        "method": { "type": "string", "const": "order.report.fill" },
        "params": {
          "type": "object",
          "properties": {
            "sender_id": { "type": "string", "const": "XCANG3" },
            "target_id": { "type": "string" },
            "transact_time": { "type": "string", "format": "date-time" },
            "cl_ord_id": { "type": "string" },
            "ex_ord_id": { "type": "string" },
            "exec_id": { "type": "string" },
            "symbol": { "type": "string" },
            "side": { "type": "string", "enum": ["buy", "sell"] },
            "qty": { "type": "integer" },
            "leaves_qty": { "type": "integer", "const": 0 },
            "cum_qty": { "type": "integer" },
            "last_shares": { "type": "integer" },
            "last_px": { "type": "number" },
            "avg_px": { "type": "number" }
          },
          "required": [
            "sender_id",
            "target_id",
            "transact_time",
            "cl_ord_id",
            "ex_ord_id",
            "exec_id",
            "symbol",
            "side",
            "qty",
            "leaves_qty",
            "cum_qty",
            "last_shares",
            "last_px",
            "avg_px"
          ]
        }
      },
      "required": ["method", "params"]
    },
    "id": { "type": "integer" }
  },
  "required": ["jsonrpc", "result", "id"]
}
```

#### XCANG3 Synchronous Response (Business Logic Order Reject)

```json
{
  "jsonrpc": "2.0",
  "result": {
    "method": "order.report.rejected",
    "params": {
      "sender_id": "XCANG3",
      "target_id": "CLIENT01",
      "transact_time": "2026-06-04T14:33:06.000Z",
      "cl_ord_id": "CL_ORD_03",
      "ex_ord_id": "NONE",
      "exec_id": "REJ_771",
      "symbol": "AAPL",
      "side": "sell",
      "qty": 100,
      "reject_reason": "Insufficient margin balance"
    }
  },
  "id": 10003
}
```

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "result": {
      "type": "object",
      "properties": {
        "method": { "type": "string", "const": "order.report.rejected" },
        "params": {
          "type": "object",
          "properties": {
            "sender_id": { "type": "string", "const": "XCANG3" },
            "target_id": { "type": "string" },
            "transact_time": { "type": "string", "format": "date-time" },
            "cl_ord_id": { "type": "string" },
            "ex_ord_id": { "type": "string" },
            "exec_id": { "type": "string" },
            "symbol": { "type": "string" },
            "side": { "type": "string", "enum": ["buy", "sell"] },
            "qty": { "type": "integer" },
            "reject_reason": { "type": "string" }
          },
          "required": [
            "sender_id",
            "target_id",
            "transact_time",
            "cl_ord_id",
            "ex_ord_id",
            "exec_id",
            "symbol",
            "side",
            "qty",
            "reject_reason"
          ]
        }
      },
      "required": ["method", "params"]
    },
    "id": { "type": "integer" }
  },
  "required": ["jsonrpc", "result", "id"]
}
```


## 3. Server-Initiated Asynchronous Notification

Once the gateway routes orders to the core matching engine, downstream execution and reject states are pushed as Server-Initiated Notifications.

### 3.1 Partially Filled Report (order.report.partial)

#### XCANG3 Notification

```json
{
  "jsonrpc": "2.0",
  "method": "order.report.partial",
  "params": {
    "sender_id": "XCANG3",
    "target_id": "CLIENT01",
    "transact_time": "2026-06-04T14:35:00.000Z",
    "ex_ord_id": "EX_ORD_99",
    "cl_ord_id": "CL_ORD_03",
    "exec_id": "FILL_882",
    "symbol": "AAPL",
    "side": "buy",
    "qty": 100,
    "leaves_qty": 60,
    "cum_qty": 40,
    "last_shares": 40,
    "last_px": 150.25,
    "avg_px": 150.25,
    "price": 150.25,
  }
}
```

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "method": { "type": "string", "const": "order.report.partial" },
    "params": {
      "type": "object",
      "properties": {
        "sender_id": { "type": "string", "const": "XCANG3" },
        "target_id": { "type": "string" },
        "transact_time": { "type": "string", "format": "date-time" },
        "ex_ord_id": { "type": "string" },
        "cl_ord_id": { "type": "string" },
        "exec_id": { "type": "string" },
        "symbol": { "type": "string" },
        "side": { "type": "string", "enum": ["buy", "sell"] },
        "qty": { "type": "integer" },
        "leaves_qty": { "type": "integer", "minimum": 1 },
        "cum_qty": { "type": "integer" },
        "last_shares": { "type": "integer" },
        "last_px": { "type": "number" },
        "avg_px": { "type": "number" },
        "price": { "type": "number", "minimum": 0.0001 },
      },
      "required": [
        "sender_id",
        "target_id",
        "transact_time",
        "ex_ord_id",
        "cl_ord_id",
        "exec_id",
        "symbol",
        "side",
        "qty",
        "leaves_qty",
        "cum_qty",
        "last_shares",
        "last_px",
        "avg_px",
        "price"
      ]
    }
  },
  "required": ["jsonrpc", "method", "params"]
}
```

### 3.2 Fully Filled Report (order.report.fill)

#### XCANG3 Notification

```json
{
  "jsonrpc": "2.0",
  "method": "order.report.fill",
  "params": {
    "sender_id": "XCANG3",
    "target_id": "CLIENT01",
    "transact_time": "2026-06-04T14:35:05.000Z",
    "ex_ord_id": "EX_ORD_99",
    "cl_ord_id": "CL_ORD_03",
    "exec_id": "FILL_883",
    "symbol": "AAPL",
    "side": "buy",
    "qty": 100,
    "leaves_qty": 0,
    "cum_qty": 100,
    "last_shares": 60,
    "last_px": 150.25,
    "avg_px": 150.25,
    "price": 150.25
  }
}
```

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "method": { "type": "string", "const": "order.report.fill" },
    "params": {
      "type": "object",
      "properties": {
        "sender_id": { "type": "string", "const": "XCANG3" },
        "target_id": { "type": "string" },
        "transact_time": { "type": "string", "format": "date-time" },
        "ex_ord_id": { "type": "string" },
        "cl_ord_id": { "type": "string" },
        "exec_id": { "type": "string" },
        "symbol": { "type": "string" },
        "side": { "type": "string", "enum": ["buy", "sell"] },
        "qty": { "type": "integer" },
        "leaves_qty": { "type": "integer", "const": 0 },
        "cum_qty": { "type": "integer" },
        "last_shares": { "type": "integer" },
        "last_px": { "type": "number" },
        "avg_px": { "type": "number" },
        "price": { "type": "number", "minimum": 0.0001 },
      },
      "required": [
        "sender_id",
        "target_id",
        "transact_time",
        "ex_ord_id",
        "cl_ord_id",
        "exec_id",
        "symbol",
        "side",
        "qty",
        "leaves_qty",
        "cum_qty",
        "last_shares",
        "last_px",
        "avg_px",
        "price",
      ]
    }
  },
  "required": ["jsonrpc", "method", "params"]
}
```

### 3.3 Business Logic Order Reject Report (order.report.rejected)

#### XCANG3 Notification

```json
{
  "jsonrpc": "2.0",
  "method": "order.report.rejected",
  "params": {
    "sender_id": "XCANG3",
    "target_id": "CLIENT01",
    "transact_time": "2026-06-04T14:33:06.000Z",
    "cl_ord_id": "CL_ORD_03",
    "ex_ord_id": "NONE",
    "exec_id": "REJ_771",
    "symbol": "AAPL",
    "side": "sell",
    "qty": 100,
    "reject_reason": "Insufficient margin balance",
    "price": 150.25,
  }
}
```

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "jsonrpc": { "type": "string", "const": "2.0" },
    "method": { "type": "string", "const": "order.report.rejected" },
    "params": {
      "type": "object",
      "properties": {
        "sender_id": { "type": "string", "const": "XCANG3" },
        "target_id": { "type": "string" },
        "transact_time": { "type": "string", "format": "date-time" },
        "cl_ord_id": { "type": "string" },
        "ex_ord_id": { "type": "string" },
        "exec_id": { "type": "string" },
        "symbol": { "type": "string" },
        "side": { "type": "string", "enum": ["buy", "sell"] },
        "qty": { "type": "integer" },
        "reject_reason": { "type": "string" },
        "price": { "type": "number", "minimum": 0.0001 },
      },
      "required": [
        "sender_id",
        "target_id",
        "transact_time",
        "cl_ord_id",
        "ex_ord_id",
        "exec_id",
        "symbol",
        "side",
        "qty",
        "reject_reason",
        "price",
      ]
    }
  },
  "required": ["jsonrpc", "method", "params"]
}
```

<!-- ## 4. Order Cancellation (FIX MsgType \= F / 9\)

### 4.1 Order Cancel Request (order.cancel)

#### CLIENT Request

{
"jsonrpc": "2.0",
"method": "order.cancel",
"params": {
"sender_id": "CLIENT01",
"target_id": "XCANG3",
"transact_time": "2026-06-04T14:37:00.000Z",
"orig_cl_ord_id": "CL_ORD_03",
"cl_ord_id": "CL_CAN_01",
"symbol": "AAPL",
"side": "buy",
"qty": 100
},
"id": 10005
}

{
"$schema": "http://json-schema.org/draft-07/schema#",
"type": "object",
"properties": {
"jsonrpc": { "type": "string", "const": "2.0" },
"method": { "type": "string", "const": "order.cancel" },
"params": {
"type": "object",
"properties": {
"sender_id": { "type": "string" },
"target_id": { "type": "string", "const": "XCANG3" },
"transact_time": { "type": "string", "format": "date-time" },
"orig_cl_ord_id": { "type": "string" },
"cl_ord_id": { "type": "string" },
"symbol": { "type": "string" },
"side": { "type": "string", "enum": ["buy", "sell"] },
"qty": { "type": "integer", "minimum": 1 }
},
"required": ["sender_id", "target_id", "transact_time", "orig_cl_ord_id", "cl_ord_id", "symbol", "side", "qty"]
},
"id": { "type": ["string", "integer"] }
},
"required": ["jsonrpc", "method", "params", "id"]
}

#### XCANG3 Synchronous Response (Immediate Success Ack)

{
"jsonrpc": "2.0",
"result": {
"cl_ord_id": "CL_CAN_01",
"orig_cl_ord_id": "CL_ORD_03",
"status": "cancel_pending",
"received_time": "2026-06-04T14:37:00.080Z"
},
"id": 10005
}

{
"$schema": "http://json-schema.org/draft-07/schema#",
"type": "object",
"properties": {
"jsonrpc": { "type": "string", "const": "2.0" },
"result": {
"type": "object",
"properties": {
"cl_ord_id": { "type": "string" },
"orig_cl_ord_id": { "type": "string" },
"status": { "type": "string", "const": "cancel_pending" },
"received_time": { "type": "string", "format": "date-time" }
},
"required": ["cl_ord_id", "orig_cl_ord_id", "status", "received_time"]
},
"id": { "type": ["string", "integer"] }
},
"required": ["jsonrpc", "result", "id"]
}

### 4.2 Asynchronous Canceled Confirmation (order.report.canceled)

#### XCANG3 Request

{
"jsonrpc": "2.0",
"method": "order.report.canceled",
"params": {
"sender_id": "XCANG3",
"target_id": "CLIENT01",
"transact_time": "2026-06-04T14:37:02.000Z",
"order_id": "EX_ORD_99",
"cl_ord_id": "CL_CAN_01",
"orig_cl_ord_id": "CL_ORD_03",
"exec_id": "CAN_999",
"ord_status": "canceled",
"symbol": "AAPL",
"side": "buy",
"qty": 100,
"leaves_qty": 0,
"cum_qty": 0
},
"id": 20004
}

{
"$schema": "http://json-schema.org/draft-07/schema#",
"type": "object",
"properties": {
"jsonrpc": { "type": "string", "const": "2.0" },
"method": { "type": "string", "const": "order.report.canceled" },
"params": {
"type": "object",
"properties": {
"sender_id": { "type": "string", "const": "XCANG3" },
"target_id": { "type": "string" },
"transact_time": { "type": "string", "format": "date-time" },
"order_id": { "type": "string" },
"cl_ord_id": { "type": "string" },
"orig_cl_ord_id": { "type": "string" },
"exec_id": { "type": "string" },
"ord_status": { "type": "string", "const": "canceled" },
"symbol": { "type": "string" },
"side": { "type": "string", "enum": ["buy", "sell"] },
"qty": { "type": "integer" },
"leaves_qty": { "type": "integer", "const": 0 },
"cum_qty": { "type": "integer", "const": 0 }
},
"required": ["sender_id", "target_id", "transact_time", "order_id", "cl_ord_id", "orig_cl_ord_id", "exec_id", "ord_status", "symbol", "side", "qty", "leaves_qty", "cum_qty"]
},
"id": { "type": ["string", "integer"] }
},
"required": ["jsonrpc", "method", "params", "id"]
}

#### CLIENT Synchronous Response

{
"jsonrpc": "2.0",
"result": {
"status": "received"
},
"id": 20004
}

### 4.3 Asynchronous Order Cancel Reject (order.report.cancel_reject)

#### XCANG3 Request

{
"jsonrpc": "2.0",
"method": "order.report.cancel_reject",
"params": {
"sender_id": "XCANG3",
"target_id": "CLIENT01",
"transact_time": "2026-06-04T14:37:30.000Z",
"order_id": "NONE",
"cl_ord_id": "CL_CAN_01",
"orig_cl_ord_id": "CL_ORD_03",
"ord_status": "rejected",
"cxl_rej_reason": "unknown_order"
},
"id": 20005
}

{
"$schema": "http://json-schema.org/draft-07/schema#",
"type": "object",
"properties": {
"jsonrpc": { "type": "string", "const": "2.0" },
"method": { "type": "string", "const": "order.report.cancel_reject" },
"params": {
"type": "object",
"properties": {
"sender_id": { "type": "string", "const": "XCANG3" },
"target_id": { "type": "string" },
"transact_time": { "type": "string", "format": "date-time" },
"order_id": { "type": "string" },
"cl_ord_id": { "type": "string" },
"orig_cl_ord_id": { "type": "string" },
"ord_status": { "type": "string", "const": "rejected" },
"cxl_rej_reason": { "type": "string" }
},
"required": ["sender_id", "target_id", "transact_time", "order_id", "cl_ord_id", "orig_cl_ord_id", "ord_status", "cxl_rej_reason"]
},
"id": { "type": ["string", "integer"] }
},
"required": ["jsonrpc", "method", "params", "id"]
}

#### CLIENT Synchronous Response

{
"jsonrpc": "2.0",
"result": {
"status": "received"
},
"id": 20005
} -->
