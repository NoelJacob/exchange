# XCANG3 Exchange — FIX 4.2 Protocol Specification

This schema defines the supported messages, raw formats, and template layouts for client-to-exchange interaction.

### Rules of the Schema:

* Fields enclosed in angle brackets with an assignment (e.g., <FieldName = Value>) are **static/constant values** that do not change for that context.
* Fields enclosed in angle brackets without an assignment (e.g., <FieldName>) are **variable parameters** generated at runtime.
* The piping character | represents the ASCII 0x01 SOH delimiter.
* Extra/unknown fields received by the exchange should be safely ignored.

### Developer Implementation Notes:

1. Body Length (Tag 9) Calculation: Start counting bytes directly after the semicolon of Tag 9 (9=...|) up to and including the SOH delimiter directly preceding Tag 10 (...|10=).
2. Checksum (Tag 10) Calculation: Sum the ASCII values of all characters in the message up to (but not including) the Tag 10 key-value pair (10=...). Take this sum modulo 256, and format it as a 3-character zero-padded string (e.g., 064 or 112).

## 1) Logon (35=A)

### CLIENT Initiates:

8=FIX.4.2|9=73|35=A|49=CLIENT01|56=XCANG3|34=1|52=20260604-14:30:00|98=0|108=30|10=112

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = A>|49=<SenderCompID>|56=<TargetCompID = XCANG3>|34=<MsgSeqNum>|52=<SendingTime>|98=<EncryptMethod = 0>|108=<HeartBtInt>|10=<CheckSum>

### XCANG3 Response:

8=FIX.4.2|9=73|35=A|49=XCANG3|56=CLIENT01|34=1|52=20260604-14:30:01|98=0|108=30|10=112

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = A>|49=<SenderCompID = XCANG3>|56=<TargetCompID>|34=<MsgSeqNum>|52=<SendingTime>|98=<EncryptMethod = 0>|108=<HeartBtInt>|10=<CheckSum>

## 2) Heartbeat (35=0)

### CLIENT Initiates (or Responds):

8=FIX.4.2|9=54|35=0|49=CLIENT01|56=XCANG3|34=2|52=20260604-14:30:30|10=064

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = 0>|49=<SenderCompID>|56=<TargetCompID = XCANG3>|34=<MsgSeqNum>|52=<SendingTime>|10=<CheckSum>

### XCANG3 Response (or Initiates):

8=FIX.4.2|9=54|35=0|49=XCANG3|56=CLIENT01|34=2|52=20260604-14:30:31|10=064

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = 0>|49=<SenderCompID = XCANG3>|56=<TargetCompID>|34=<MsgSeqNum>|52=<SendingTime>|10=<CheckSum>

## 3) Test Request (35=1)

### CLIENT Initiates:

8=FIX.4.2|9=69|35=1|49=CLIENT01|56=XCANG3|34=3|52=20260604-14:31:00|112=TEST_99|10=188

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = 1>|49=<SenderCompID>|56=<TargetCompID = XCANG3>|34=<MsgSeqNum>|52=<SendingTime>|112=<TestReqID>|10=<CheckSum>

### XCANG3 Response (Heartbeat matching incoming TestReqID):

8=FIX.4.2|9=69|35=0|49=XCANG3|56=CLIENT01|34=3|52=20260604-14:31:01|112=TEST_99|10=188

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = 0>|49=<SenderCompID = XCANG3>|56=<TargetCompID>|34=<MsgSeqNum>|52=<SendingTime>|112=<TestReqID>|10=<CheckSum>

## 4) Session Reject (35=3)

### XCANG3 Response (Malformed / Missing Structural Tag):

8=FIX.4.2|9=63|35=3|49=XCANG3|56=CLIENT01|34=4|52=20260604-14:31:15|45=3|10=201

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = 3>|49=<SenderCompID = XCANG3>|56=<TargetCompID>|34=<MsgSeqNum>|52=<SendingTime>|45=<RefSeqNum>|10=<CheckSum>

## 5) Logout (35=5)

### CLIENT Initiates:

8=FIX.4.2|9=54|35=5|49=CLIENT01|56=XCANG3|34=4|52=20260604-14:32:00|10=069

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = 5>|49=<SenderCompID>|56=<TargetCompID = XCANG3>|34=<MsgSeqNum>|52=<SendingTime>|10=<CheckSum>

### XCANG3 Response:

8=FIX.4.2|9=54|35=5|49=XCANG3|56=CLIENT01|34=5|52=20260604-14:32:01|10=069

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = 5>|49=<SenderCompID = XCANG3>|56=<TargetCompID>|34=<MsgSeqNum>|52=<SendingTime>|10=<CheckSum>

## 6) Buy Market Order (35=D)

### CLIENT Initiates:

8=FIX.4.2|9=99|35=D|49=CLIENT01|56=XCANG3|34=5|52=20260604-14:33:00|11=CL_ORD_01|21=1|55=AAPL|54=1|60=20260604-14:33:00|38=100|40=1|10=042

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = D>|49=<SenderCompID>|56=<TargetCompID = XCANG3>|34=<MsgSeqNum>|52=<SendingTime>|11=<ClOrdID>|21=<HandlInst = 1>|55=<Symbol>|54=<Side = 1>|60=<TransactTime>|38=<OrderQty>|40=<OrdType = 1>|10=<CheckSum>

## 7) Sell Market Order (35=D)

### CLIENT Initiates:

8=FIX.4.2|9=99|35=D|49=CLIENT01|56=XCANG3|34=6|52=20260604-14:33:05|11=CL_ORD_02|21=1|55=AAPL|54=2|60=20260604-14:33:05|38=100|40=1|10=045

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = D>|49=<SenderCompID>|56=<TargetCompID = XCANG3>|34=<MsgSeqNum>|52=<SendingTime>|11=<ClOrdID>|21=<HandlInst = 1>|55=<Symbol>|54=<Side = 2>|60=<TransactTime>|38=<OrderQty>|40=<OrdType = 1>|10=<CheckSum>

## 8) Buy Limit Order (35=D)

### CLIENT Initiates:

8=FIX.4.2|9=108|35=D|49=CLIENT01|56=XCANG3|34=7|52=20260604-14:34:00|11=CL_ORD_03|21=1|55=AAPL|54=1|60=20260604-14:34:00|38=100|40=2|44=150.25|10=210

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = D>|49=<SenderCompID>|56=<TargetCompID = XCANG3>|34=<MsgSeqNum>|52=<SendingTime>|11=<ClOrdID>|21=<HandlInst = 1>|55=<Symbol>|54=<Side = 1>|60=<TransactTime>|38=<OrderQty>|40=<OrdType = 2>|44=<Price>|10=<CheckSum>

## 9) Sell Limit Order (35=D)

### CLIENT Initiates:

8=FIX.4.2|9=108|35=D|49=CLIENT01|56=XCANG3|34=8|52=20260604-14:34:10|11=CL_ORD_04|21=1|55=AAPL|54=2|60=20260604-14:34:10|38=100|40=2|44=152.00|10=215

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = D>|49=<SenderCompID>|56=<TargetCompID = XCANG3>|34=<MsgSeqNum>|52=<SendingTime>|11=<ClOrdID>|21=<HandlInst = 1>|55=<Symbol>|54=<Side = 2>|60=<TransactTime>|38=<OrderQty>|40=<OrdType = 2>|44=<Price>|10=<CheckSum>

## 10) Order Response: Acknowledged / New (35=8)

### XCANG3 Response (Sent immediately upon accepting a valid **limit** order to the book):

8=FIX.4.2|9=148|35=8|49=XCANG3|56=CLIENT01|34=5|52=20260604-14:34:12|37=EX_ORD_99|11=CL_ORD_03|17=ACK_001|20=0|150=0|39=0|55=AAPL|54=1|38=100|151=100|14=0|6=0|10=051

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = 8>|49=<SenderCompID = XCANG3>|56=<TargetCompID>|34=<MsgSeqNum>|52=<SendingTime>|37=<OrderID>|11=<ClOrdID>|17=<ExecID>|20=<ExecTransType = 0>|150=<ExecType = 0>|39=<OrdStatus = 0>|55=<Symbol>|54=<Side>|38=<OrderQty>|151=<LeavesQty>|14=<CumQty>|6=<AvgPx = 0>|60=<TransactTime>|44=<Price for limit>|40=<OrdType>|10=<CheckSum>

## 11) Order Response: Partially Filled Execution Report (35=8)

### XCANG3 Response:

8=FIX.4.2|9=172|35=8|49=XCANG3|56=CLIENT01|34=6|52=20260604-14:35:00|37=EX_ORD_99|11=CL_ORD_03|17=FILL_882|20=0|150=1|39=1|55=AAPL|54=1|38=100|151=60|14=40|32=40|31=150.25|6=150.25|10=181

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = 8>|49=<SenderCompID = XCANG3>|56=<TargetCompID>|34=<MsgSeqNum>|52=<SendingTime>|37=<OrderID>|11=<ClOrdID>|17=<ExecID>|20=<ExecTransType = 0>|150=<ExecType = 1>|39=<OrdStatus = 1>|55=<Symbol>|54=<Side>|38=<OrderQty>|151=<LeavesQty>|14=<CumQty>|32=<LastShares>|31=<LastPx>|6=<AvgPx>|60=<TransactTime>|44=<Price for limit>|40=<OrdType>|29=<LastCapacity = 1>|30=<LastMkt = XCANG3>|10=<CheckSum>

## 12) Order Response: Fully Filled Execution Report (35=8)

### XCANG3 Response:

8=FIX.4.2|9=171|35=8|49=XCANG3|56=CLIENT01|34=7|52=20260604-14:35:05|37=EX_ORD_99|11=CL_ORD_03|17=FILL_883|20=0|150=2|39=2|55=AAPL|54=1|38=100|151=0|14=100|32=60|31=150.25|6=150.25|10=192

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = 8>|49=<SenderCompID = XCANG3>|56=<TargetCompID>|34=<MsgSeqNum>|52=<SendingTime>|37=<OrderID>|11=<ClOrdID>|17=<ExecID>|20=<ExecTransType = 0>|150=<ExecType = 2>|39=<OrdStatus = 2>|55=<Symbol>|54=<Side>|38=<OrderQty>|151=<LeavesQty>|14=<CumQty>|32=<LastShares>|31=<LastPx>|6=<AvgPx>|60=<TransactTime>|44=<Price for limit>|40=<OrdType>|29=<LastCapacity = 1>|30=<LastMkt = XCANG3>|10=<CheckSum>

## 13) Order Response: Business Logic Order Reject (35=8)

### XCANG3 Response:

8=FIX.4.2|9=138|35=8|49=XCANG3|56=CLIENT01|34=8|52=20260604-14:36:00|37=NONE|11=CL_ORD_01|17=REJ_771|20=0|150=8|39=8|55=INVALID|54=1|38=100|151=0|14=0|6=0|58=NO LIQUIDITY|10=099

8=<BeginString = FIX.4.2>|9=<BodyLength>|35=<MsgType = 8>|49=<SenderCompID = XCANG3>|56=<TargetCompID>|34=<MsgSeqNum>|52=<SendingTime>|37=<OrderID = NONE>|11=<ClOrdID>|17=<ExecID>|20=<ExecTransType = 0>|150=<ExecType = 8>|39=<OrdStatus = 8>|55=<Symbol>|54=<Side>|38=<OrderQty>|151=<LeavesQty>|14=<CumQty>|6=<AvgPx = 0>|60=<TransactTime>|44=<Price for limit>|40=<OrdType>|58=<Text>|10=<CheckSum>
