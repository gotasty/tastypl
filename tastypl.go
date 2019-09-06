// Copyright (C) 2017  Go Tasty
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/shopspring/decimal"
	chart "github.com/wcharczuk/go-chart"
	"github.com/wcharczuk/go-chart/util"
)

type transaction struct {
	// Initial group of fields straight from the CSV.  Those should be
	// considered immutable, don't change the values imported from the CSV.
	date        time.Time
	txType      string
	action      string
	symbol      string // OCC symbol for options, or underlying name otherwise
	instrument  string
	description string
	value       decimal.Decimal
	quantity    decimal.Decimal
	avgPrice    decimal.Decimal
	commission  decimal.Decimal
	fees        decimal.Decimal
	multiplier  uint16
	underlying  string
	expDate     time.Time
	strike      decimal.Decimal

	// Various flags inferred from the transactions to help make things simpler.
	option bool // or non-option (such as future/equity) if false
	future bool // true if futures (or options on futures), else equity or index
	mtm    bool // mark-to-market daily settlement for futures
	call   bool // or put if false
	long   bool // or short if false
	open   bool // or closing transaction if false.  Only used on options.

	// Starts as quantity, decremented as we close the position.
	// Only really meaningful for opening transactions.
	qtyOpen decimal.Decimal
	// If this is an opening options transaction and we detected that this was
	// the result of a roll, this points to the closing transaction of the
	// position we rolled from.
	rolledFrom *transaction
	// Realized P&L on this transaction.
	// Only really meaningful for closing transactions.
	rpl decimal.Decimal
	// Opening transaction from which the realized P&L was made.
	// Only really meaningful for closing transactions.
	// Exception: for equity trades resulting from options assignment/exercise,
	// this points to the trade of the option's assignment exercise.
	openTx *transaction
}

var (
	oneHundred  = decimal.New(100, 0)
	oneThousand = decimal.New(1000, 0)

	// Cutoff point for YTD stuff.
	ytdStart = time.Date(time.Now().Year(), 1, 1, 0, 0, 0, 0, time.Now().Location())

	indexSymbols = map[string]struct{}{
		"SPX": struct{}{},
		"RUT": struct{}{},
		"DJX": struct{}{},
		"OEX": struct{}{},
		"VIX": struct{}{},
		"NDX": struct{}{},
		"MNX": struct{}{},
		"RVX": struct{}{},
		"XSP": struct{}{},
		"XEO": struct{}{},
	}

	// How many dollars is one point worth.
	// This is madness. Can't wait for the Small Exchange.
	futuresPoints = map[string]decimal.Decimal{
		// Equities
		"/ES":  decimal.RequireFromString("50"),
		"/MES": decimal.RequireFromString("5"),
		"/YM":  decimal.RequireFromString("5"),
		"/MYM": decimal.RequireFromString("0.5"),
		"/NQ":  decimal.RequireFromString("20"),
		"/MNQ": decimal.RequireFromString("2"),
		"/RTY": decimal.RequireFromString("50"),
		"/M2K": decimal.RequireFromString("5"),
		// Yield curve
		"/GE": decimal.RequireFromString("2500"),
		"/ZT": decimal.RequireFromString("2000"),
		"/ZF": decimal.RequireFromString("1000"),
		"/ZN": decimal.RequireFromString("1000"),
		"/ZB": decimal.RequireFromString("1000"),
		// Metals
		"/GC":  decimal.RequireFromString("100"),
		"/MGC": decimal.RequireFromString("10"),
		"/SI":  decimal.RequireFromString("5000"),
		// Energy
		"/CL": decimal.RequireFromString("1000"),
		"/QM": decimal.RequireFromString("500"),
		"/NG": decimal.RequireFromString("10000"),
		"/QG": decimal.RequireFromString("2500"),
		// Ags
		"/ZC": decimal.RequireFromString("50"),
		"/ZS": decimal.RequireFromString("50"),
		"/ZW": decimal.RequireFromString("50"),
		// FX
		"/6A":  decimal.RequireFromString("100000"),
		"/M6A": decimal.RequireFromString("10000"),
		"/6B":  decimal.RequireFromString("62500"),
		"/M6B": decimal.RequireFromString("6250"),
		"/6C":  decimal.RequireFromString("100000"),
		"/6E":  decimal.RequireFromString("125000"),
		"/M6E": decimal.RequireFromString("12500"),
		"/6J":  decimal.RequireFromString("125000000"),
		// Crypto
		"/BTC": decimal.RequireFromString("5"),
	}
)

func (t *transaction) String() string {
	return t.description
}

// NetCredit is the net credit after rolls on a per contract basis.
func (t *transaction) NetCredit() decimal.Decimal {
	net := t.avgPrice
	earlier := t.rolledFrom
	for earlier != nil {
		net = net.Add(earlier.rpl.Div(earlier.quantity))
		earlier = earlier.openTx.rolledFrom
	}
	return net
}

// per contract fees, see:
// https://tastyworks.desk.com/customer/en/portal/articles/2696746-commissions-and-fees-breakdown
var (
	clearingFee = decimal.New(10, -2) // -0.10
	// Options Regulatory Fee (rounded to nearest cent)
	orFee = decimal.New(415, -4) // -0.0415
	// Trading Activity Fee (only for sales, rounded up)
	finraTAF = decimal.New(2, -3) // -0.002
	// SEC Regulatory Fee (on notional amount, only for sales, rounded up)
	secFee = decimal.New(231, -7) // -0.0000231
)

func roundUp(d decimal.Decimal) decimal.Decimal {
	return d.Mul(oneHundred).Ceil().Div(oneHundred)
}

// There is a rounding error in the fees reported by the desktop app and
// web-based account management interface.  Here we recompute the correct
// amount and fix the transaction.
// TODO: This computes higher fees than it should for some reason.
func (t *transaction) fixFees() {
	if t.txType != "Trade" {
		return
	}
	fees := clearingFee.Mul(t.quantity)
	fees = fees.Add(orFee.Mul(t.quantity).Round(2))
	if !t.long {
		fees = fees.Add(roundUp(finraTAF.Mul(t.quantity)))
		fees = fees.Add(roundUp(secFee.Mul(t.value)))
	}
	fees = fees.Neg()
	if !t.fees.Equal(fees) {
		glog.V(5).Infof("fee %s doesn't matched computed fee %s in %s", t.fees, fees, t)
		t.fees = fees
	}
}

func (t *transaction) sanityCheck() {
	if t.quantity.Sign() <= 0 {
		glog.Fatalf("quantity is negative in %s", t)
	}
	if !t.option {
		return // TODO: no other check yet for futures/equities
	} else if t.mtm {
		glog.Fatalf("options position can't be marked-to-market in %s", t)
	} else if t.multiplier == 0 {
		glog.Fatalf("options position must have a non-zero multiplier in %s", t)
	} else if !t.quantity.Equal(t.quantity.Truncate(0)) {
		glog.Fatalf("options quantity must be a whole number in %s", t)
	}
	if t.strike.LessThanOrEqual(decimal.Zero) {
		glog.Fatalf("strike price can't be less than or equal to zero in %s", t)
	}
	if t.date.After(t.expDate) {
		glog.Fatalf("transaction date %s happened after expiration %s in %s",
			t.date, t.expDate, t)
	}
	if t.future {
		return // TODO: symbology for options on futures
	}
	var cp byte
	if t.call {
		cp = 'C'
	} else {
		cp = 'P'
	}
	// Recompute OCC symbol and ensure what we have is the same
	// See https://www.theocc.com/components/docs/initiatives/symbology/symbology_initiative_v1_8.pdf
	strike := t.strike.Mul(oneThousand).IntPart()
	expDate := t.expDate.Format("060102")
	const symfmt = "%- 6s%s%c%08d"
	symbol := fmt.Sprintf(symfmt, t.underlying, expDate, cp, strike)
	if symbol != t.symbol {
		if _, index := indexSymbols[t.underlying]; !index {
			glog.Fatalf("expected symbol %q but found %q in %s", symbol, t.symbol, t)
		}
		// PM-settled index options have an additional "P" in the symbol.
		// This is a hack to accept those.
		pm := t.underlying + "P"
		symbol = fmt.Sprintf(symfmt, pm, expDate, cp, strike)
		if symbol != t.symbol {
			// Weekly index options have an additional "W" in the symbol.
			// This is another hack to accept those.
			wk := t.underlying + "W"
			symbol = fmt.Sprintf(symfmt, wk, expDate, cp, strike)
			if symbol != t.symbol {
				glog.Fatalf("expected symbol %q but found %q in %s", symbol, t.symbol, t)
			}
		}
	}
}

func (t *transaction) parsePrice() decimal.Decimal {
	// Parse the open price from the description :-/
	at := strings.IndexByte(t.description, '@')
	var adj decimal.Decimal
	if at == -1 {
		prefix := "Removal of " + t.underlying + " due to expiration, last mark:"
		if !strings.HasPrefix(t.description, prefix) {
			glog.Fatal("Can't infer transaction price from %s", t)
		}
		at = len(prefix) - 1
		contract := t.underlying[:len(t.underlying)-2]
		pointValue, ok := futuresPoints[contract]
		if !ok {
			glog.Fatalf("Don't know how much a point of %s is worth in %s", contract, t)
		}
		adj = t.value.Div(pointValue)
	}
	p := t.description[at+2:]
	price, err := decimal.NewFromString(p)
	if err != nil {
		glog.Fatalf("Can't parse opening price of futures transaction %s: %s", t, err)
	}
	return price.Add(adj)
}

// Helper function to work around the fact that some CSV records for options
// on futures are missing fields we need but that we can thankfully extract
// from the symbol.
func parseFuturesOptionSymbol(rec []string) {
	// e.g.: Sold 1 /6EZ8 EUUV8 10/05/18 Put 1.15 @ 0.0027
	// Symbol: "./6EZ8 EUUV8 181005P1.15" -- not sure why the leading dot there.
	symbol := rec[3]
	if symbol[0] != '/' {
		glog.Fatalf("unexpected symbol for option on futures: %q", rec)
	}
	sym := strings.Split(symbol, " ")
	// Synthesize missing fields from the CSV:
	rec[11] = "1"    // Multiplier
	rec[12] = sym[0] // Underlying

	last := sym[len(sym)-1]                                 // <YYMMDD><P|C><strike>
	rec[13] = last[2:4] + "/" + last[4:6] + "/" + last[0:2] // Expiration
	rec[14] = last[7:]                                      // Strike
	switch last[6] {
	case 'C':
		rec[15] = "CALL"
	case 'P':
		rec[15] = "PUT"
	default:
		glog.Fatalf("couldn't find P or C in symbol: %q", rec)
	}
}

func (t *transaction) ytd() bool {
	return t.date.After(ytdStart)
}

func loadCSV(path string) [][]string {
	f, err := os.Open(path)
	if err != nil {
		glog.Fatal(err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		glog.Fatal(err)
	}
	return records
}

func parseDecimal(value string) decimal.Decimal {
	if value == "" {
		return decimal.Decimal{}
	}
	d, err := decimal.NewFromString(strings.Replace(value, ",", "", -1))
	if err != nil {
		glog.Fatal(err)
	}
	return d
}

const almostRFC3339 = "2006-01-02T15:04:05-0700"

type position struct {
	// Opening transaction(s) for this position
	opens []*transaction
}

// We consider other transactions within this time window as candidates
// when trying to detect rolls.
const rollWindow = 30 * time.Second

// Used to track recent transactions to detect rolls (for options only).
type recentKey struct {
	underlying string
	call       bool // or put if false
	long       bool // or short if false
	open       bool // or closing transaction if false.
}

type portfolio struct {
	ytd bool // Only track YTD transactions

	ignoreacat bool // Ignore ACAT transactions

	// All transactions.
	transactions []*transaction

	// Recent transactions, used to detect rolls of options positions.
	// Transactions older than rollWindow should get purged.
	recentTx map[recentKey][]*transaction
	// Map a futures contract name to the last mark to market settlement
	// price seen for that contract.
	lastMarkToMarket map[string]decimal.Decimal

	moneyMov decimal.Decimal // sum of all ACH/wire movements

	// Map of symbol to opening transaction(s)
	positions map[string]*position

	cash decimal.Decimal // Cash on hand

	premium decimal.Decimal // Sum of premium for currently open positions.

	rpl   decimal.Decimal // Total realized P&L
	comms decimal.Decimal
	fees  decimal.Decimal
	intrs decimal.Decimal // interest
	// Misc. cash changes
	miscCash decimal.Decimal

	// Cumulative tallies
	optionsNotionalSold   decimal.Decimal
	optionsNotionalBought decimal.Decimal
	putsTraded            int64
	callsTraded           int64
	equityNotionalSold    decimal.Decimal
	equityNotionalBought  decimal.Decimal
	futuresNotionalSold   decimal.Decimal
	futuresNotionalBought decimal.Decimal

	// Summary of realized P&L per underlying
	rplPerUnderlying map[string]decimal.Decimal

	numTrades uint16 // Number of trades executed
}

func NewPortfolio(records [][]string, ytd, nofutures, ignoreacat bool) *portfolio {
	p := &portfolio{
		ytd:              ytd,
		ignoreacat:       ignoreacat,
		positions:        make(map[string]*position),
		recentTx:         make(map[recentKey][]*transaction),
		lastMarkToMarket: make(map[string]decimal.Decimal),
		rplPerUnderlying: make(map[string]decimal.Decimal),
	}

	// The CSV is sorted from newest to oldest transaction, reverse it.
	for i := len(records)/2 - 1; i >= 0; i-- {
		opp := len(records) - 1 - i
		records[i], records[opp] = records[opp], records[i]
	}
	p.parseTransactions(records, nofutures)

	var prevTime time.Time
	var prevRPL decimal.Decimal
	logDailyRPL := func() {
		if dpl := p.rpl.Sub(prevRPL); !dpl.Equal(decimal.Zero) {
			glog.V(4).Infof("Day P/L realized for %s: %9s", prevTime.Format("01/02/06"), dpl.StringFixed(2))
			prevRPL = p.rpl
		}
	}
	for i, tx := range p.transactions {
		if prevTime.After(tx.date) {
			glog.Fatalf("transaction log out of order at time %s (tx=%s)", tx.date, tx)
		} else if tx.date.Sub(prevTime) > rollWindow && len(p.recentTx) > 0 {
			// More than rollWindow time has elapsed between two transactions,
			// clear our map of recent transactions as we can't possibly find
			// any rolls in it anymore.
			p.recentTx = make(map[recentKey][]*transaction)
		}
		if prevTime.YearDay() != tx.date.YearDay() {
			logDailyRPL()
		}
		prevTime = tx.date
		// When we are handling an assignment or exercise, the transaction in the
		// underlying can appear either before or after the transaction that retires the
		// option position.  This is problematic for us, because we need to adjut the
		// cost basis or P&L by the amount of the premium, so we need to tie together
		// the two transactions somehow.  Work around this inconsistency in the CSV by
		// detecting assignment/exercise transaction pairs and making the option
		// transaction point to the equity transaction.
		if tx.txType == "Receive Deliver" && i != len(p.transactions)-1 {
			nextTx := p.transactions[i+1]
			p.maybeLinkAssignmentExercises(tx, nextTx)
		}
		p.handleTransaction(tx)
	}
	logDailyRPL()
	return p
}

func (p *portfolio) maybeLinkAssignmentExercises(tx, nextTx *transaction) {
	if !(nextTx.txType == "Receive Deliver" && tx.date.Equal(nextTx.date)) {
		return
	}
	var op *transaction
	var buy bool
	if tx.option {
		op = tx
		buy = nextTx.long
	} else if nextTx.option {
		op = nextTx
		buy = tx.long
	} else {
		return
	}
	shares := op.quantity.Mul(decimal.New(int64(op.multiplier), 0))
	price := op.strike.Mul(shares)
	if buy {
		price = price.Neg()
	}
	if tx.openTx == nil && tx.action == "" &&
		nextTx.action != "" && tx.underlying == nextTx.symbol &&
		nextTx.quantity.Equal(shares) && nextTx.value.Equal(price) {
		// First case: option transaction appears before equity transaction.
		tx.openTx = nextTx
	} else if nextTx.openTx == nil && tx.action != "" &&
		nextTx.action == "" && tx.symbol == nextTx.underlying &&
		tx.quantity.Equal(shares) && tx.value.Equal(price) {
		// Second case: equity transaction appears before option transaction.
		nextTx.openTx = tx
	}
}

func (p *portfolio) parseNonOptionTransaction(record []string) {
	switch record[1] {
	case "Money Movement":
		amount := parseDecimal(record[6])
		switch record[5] {
		case "INTEREST ON CREDIT BALANCE":
			p.intrs = p.intrs.Add(amount)
		case "ACH DEPOSIT", "ACH DISBURSEMENT", "Wire Funds Received":
			p.moneyMov = p.moneyMov.Add(amount)
		case "Regulatory fee adjustment":
			// TW incorrectly calculates some regulatory fees (!) so in order
			// to fix the small discrepancies that build up over time, they
			// reconcile the fees with what their clearing firm (Apex)
			// actually charged by adding weekly transactions to adjust
			// customer balances on their platforms. Can't believe they
			// haven't fixed this by now, this has been going on for months...
			p.fees = p.fees.Add(amount)
		default:
			if strings.HasPrefix(record[5], "FROM ") {
				// Interest paid, e.g. "FROM 10/16 THRU 11/15 @ 8    %"
				p.intrs = p.intrs.Add(amount)
				return
			} else if record[4] == "Equity" && record[3] != "" {
				// Interest paid on short stock (borrow fees)
				p.intrs = p.intrs.Add(amount)
				return
			}
			glog.V(2).Infof("unhandled money movement: %#v", record)
			p.miscCash = p.miscCash.Add(amount)
		}
	default:
		glog.V(1).Infof("unhandled %#v", record)
	}
}

// AddTransaction adds an individual transaction.  Usually all the
// transactions are passed to the constructor and bulk-imported.
// Transactions must be added in chronological order.
func (p *portfolio) AddTransaction(record []string) {
	tx := p.parseTransaction(len(p.transactions), record, &p.ytd)
	if tx == nil {
		return
	}
	p.transactions = append(p.transactions, tx)
	defer func() {
		if e := recover(); e != nil {
			panic(fmt.Errorf("when handling %s: %v", record, e))
		}
	}()

	prevTime := p.transactions[len(p.transactions)-1].date
	if prevTime.After(tx.date) {
		glog.Fatalf("adding a transaction out of order at time %s (last=%s tx=%s)",
			prevTime, tx.date, tx)
	} else if tx.date.Sub(prevTime) > rollWindow && len(p.recentTx) > 0 {
		// More than rollWindow time has elapsed between two transactions,
		// clear our map of recent transactions as we can't possibly find
		// any rolls in it anymore.
		p.recentTx = make(map[recentKey][]*transaction)
	}
	if tx.txType == "Receive Deliver" {
		prevTx := p.transactions[len(p.transactions)-2]
		p.maybeLinkAssignmentExercises(prevTx, tx)
	}
	p.handleTransaction(tx)
}

// parseTransactions parses raw transactions from the CSV and adds them to the
// transaction history.
func (p *portfolio) parseTransactions(records [][]string, nofutures bool) {
	p.transactions = make([]*transaction, len(records))
	var j int
	ytd := p.ytd
	for i, rec := range records {
		if tx := p.parseTransaction(i, rec, &ytd); tx != nil {
			if nofutures && tx.future {
				continue
			}
			p.transactions[j] = tx
			j++
		}
	}
	if ignored := len(records) - j; ignored != 0 {
		glog.V(3).Infof("Ignored %d non-option transactions", ignored)
	}
	p.transactions = p.transactions[:j]
}

func (p *portfolio) parseTransaction(i int, rec []string, ytd *bool) *transaction {
	date, err := time.Parse(almostRFC3339, rec[0])
	if err != nil {
		glog.Fatalf("record #%d, bad transaction date: %s", i, err)
	}
	if *ytd && date.After(ytdStart) {
		// Reset running counts as we only want YTD numbers.
		p.miscCash = decimal.Zero
		p.intrs = decimal.Zero
		p.moneyMov = decimal.Zero
		// Note: we don't reset p.cash as the only way to have the correct
		// amount of cash ultimately in the account is of course to take
		// into account (bad pun, sorry) the entire account history.
		*ytd = false // So we don't reset again.
	}
	value := parseDecimal(rec[6])
	comm := parseDecimal(rec[9])
	// When TW fixes up a transaction due to a bug/issue on their end, they
	// rollback a previous one, which leads to positive commission/fee amounts.
	adminTx := rec[1] == "Administrative Transfer"
	if comm.GreaterThan(decimal.Zero) && !adminTx {
		glog.Fatalf("record #%d, positive commission amount %s in %q", i, rec[9], rec)
	}
	fees := parseDecimal(rec[10])
	if fees.GreaterThan(decimal.Zero) && !adminTx {
		glog.Fatalf("record #%d, positive fees amount %s in %q", i, rec[10], rec)
	}
	p.cash = p.cash.Add(value).Add(comm).Add(fees)
	txType := rec[1]
	instrument := rec[4]
	var call bool
	var mtm bool
	option := true
	if instrument == "Future Option" && rec[3][0] == '.' {
		rec[3] = rec[3][1:] // Not sure why there is a dot there but drop it
	}
	switch rec[15] {
	case "PUT":
	case "CALL":
		call = true
	case "":
		// Handle non-trade transactions.
		if txType != "Trade" && txType != "Receive Deliver" {
			// Detect daily mark-to-market of futures held overnight
			if txType == "Money Movement" && instrument == "Future" &&
				(strings.HasSuffix(rec[5], "Final settlement price") ||
					strings.HasSuffix(rec[5], "Preliminary settlement price")) {
				mtm = true
			} else {
				p.parseNonOptionTransaction(rec)
				return nil
			}
		}
		if instrument == "Future Option" {
			// As of version v0.31.5 TW's CSV export for options on futures is a bit buggy
			// and various columns are not set, so we get here.  Work around that for now
			// by extracting the columns from the symbol.
			parseFuturesOptionSymbol(rec)
			call = rec[15] == "CALL"
		} else if strings.HasSuffix(instrument, "Option") {
			glog.Fatalf("WTF, record #%d should be a non-option transaction: %q", i, rec)
		} else {
			// fallthrough (this is a trade but a non-option transaction)
			option = false
			if txType == "Receive Deliver" && instrument == "Future" && strings.HasSuffix(rec[5], "due to expiration") {
				lastMark, ok := p.lastMarkToMarket[rec[3]]
				if !ok {
					glog.Fatalf("Future expired without ever getting marked: %s", rec)
				}
				rec[5] += ", last mark: " + lastMark.String()
			}
		}
	default:
		glog.Fatalf("record #%d, bad put/call type: %q", i, rec[15])
	}
	mult, err := strconv.ParseUint(rec[11], 10, 16)
	if option && err != nil {
		glog.Fatalf("record #%d, bad multiplier: %s", i, err)
	}
	expDate, err := time.Parse("1/02/06", rec[13])
	if option && err != nil {
		glog.Fatalf("record #%d, bad transaction date: %s", i, err)
	}
	action := rec[2]
	long := strings.HasPrefix(action, "BUY")
	// Pretend expiration is at 23:00 on the day of expiration so that
	// expDate > tx date for transactions on the day of expiration.
	if option {
		expDate = expDate.Add(23 * time.Hour)
	}
	qty := parseDecimal(rec[7])
	tx := &transaction{
		date:        date,
		txType:      txType,
		action:      action,
		symbol:      rec[3],
		instrument:  instrument,
		description: rec[5],
		value:       value,
		quantity:    qty,
		avgPrice:    parseDecimal(rec[8]),
		commission:  comm,
		fees:        fees,
		multiplier:  uint16(mult),
		underlying:  rec[12],
		expDate:     expDate,
		strike:      parseDecimal(rec[14]),
		option:      option,
		future:      strings.HasPrefix(instrument, "Future"),
		mtm:         mtm,
		call:        call,
		long:        long,
		open:        strings.HasSuffix(action, "_TO_OPEN"),
		qtyOpen:     qty,
	}
	if !option {
		tx.underlying = tx.symbol
	} else if !strings.HasPrefix(tx.symbol, tx.underlying) {
		glog.Fatalf("invalid symbol %q for underlying %q in %s",
			tx.symbol, tx.underlying, tx)
	}
	// Pending clarification from TW support.
	//tx.fixFees()
	tx.sanityCheck()
	if tx.mtm {
		p.updateSettlementPrice(tx)
	}
	return tx
}

func (p *portfolio) updateSettlementPrice(tx *transaction) {
	if !tx.mtm {
		glog.Fatalf("must pass a futures mark-to-market transaction in argument, got %s", tx)
	}
	prefix := tx.underlying + " mark to market at "
	if !strings.HasPrefix(tx.description, prefix) {
		glog.Fatalf("Unexpected futures mark-to-market tx description %q", tx)
	}
	price := tx.description[len(prefix):]
	space := strings.IndexByte(price, ' ')
	if space <= 0 {
		glog.Fatalf("Unexpected futures mark-to-market tx description %q", tx)
	}
	price = price[:space]
	mark, err := decimal.NewFromString(price)
	if err != nil {
		glog.Fatalf("Can't parse futures mark-to-market settlement value %q from %q", price, tx)
	}
	p.lastMarkToMarket[tx.underlying] = mark
}

// For a transction on an outright futures, return the notional amount of the
// price at which the transaction was filled.
func futuresNotional(tx *transaction) decimal.Decimal {
	if !tx.future {
		glog.Fatalf("expected a futures transaction but got %s", tx)
	}
	points := tx.parsePrice()
	contract := tx.underlying[:len(tx.underlying)-2]
	pointValue, ok := futuresPoints[contract]
	if !ok {
		glog.Fatalf("Don't know how much a point of %s is worth in %s", contract, tx)
	}
	notional := points.Mul(pointValue).Mul(tx.quantity)
	if tx.long {
		notional = notional.Neg()
	}
	return notional
}

func (p *portfolio) handleTrade(tx *transaction, count bool) {
	// Update cumulative stats.
	if count {
		if tx.option {
			if tx.open {
				contracts := tx.quantity.IntPart()
				if tx.call {
					p.callsTraded += contracts
				} else {
					p.putsTraded += contracts
				}
			}
			if tx.long {
				p.optionsNotionalBought = p.optionsNotionalBought.Add(tx.value)
			} else {
				p.optionsNotionalSold = p.optionsNotionalSold.Add(tx.value)
			}
		} else if tx.future { // outright futures (futures options are handled in the case above)
			notional := futuresNotional(tx)
			if tx.long {
				p.futuresNotionalBought = p.futuresNotionalBought.Add(notional)
			} else {
				p.futuresNotionalSold = p.futuresNotionalSold.Add(notional)
			}
		} else if tx.instrument == "Equity" {
			if tx.long {
				p.equityNotionalBought = p.equityNotionalBought.Add(tx.value)
			} else {
				p.equityNotionalSold = p.equityNotionalSold.Add(tx.value)
			}
		}
	}

	switch tx.action {
	case "SELL_TO_OPEN", "BUY_TO_OPEN":
		p.openPosition(tx)
		p.detectRoll(tx)
	case "SELL_TO_CLOSE", "BUY_TO_CLOSE":
		p.closePosition(tx, count)
		p.detectRoll(tx)
	default:
		if tx.future && !tx.option {
			// TODO check what happens for equities
			if tx.value.Equal(decimal.Zero) { // Open
				p.openPosition(tx)
			} else { // Settle
				p.closePosition(tx, count)
			}
		} else {
			glog.Fatalf("Unhandled action type %q in %s %#v", tx.action, tx, tx)
		}
	}
}

func (p *portfolio) handleAssignmentOrExercise(tx *transaction, count bool) {
	expired := strings.HasSuffix(tx.description, "due to expiration.")
	if !expired {
		costBasisAdj := tx.description == "Reversal for cost basis adjustment"
		if strings.HasSuffix(tx.description, "via ACAT") || costBasisAdj {
			if strings.HasPrefix(tx.description, "Removed ") {
				glog.V(4).Infof("ACAT transfer out: %s (following P&L ignored)", tx)
				p.closePosition(tx, false)
				return
			} else if costBasisAdj && !tx.open {
				glog.V(8).Infof("Ignored ACAT cost basis adjustment %s", tx)
				return
			} else if p.ignoreacat {
				glog.V(7).Infof("Ignored ACAT %s", tx)
				return
			}
		} else if strings.HasPrefix(tx.description, "Cash settlement of") { // e.g. for VIX options that expire ITM
			glog.V(4).Infof("Recording P/L for %s of %s", tx, tx.value)
			// Sucks that we have to record the P/L manually here. We can't
			// just fall through to calling handleTrade() below because that
			// will close the position. The settlement is actually two
			// transactions: one for the cash settlement, one for the
			// expiration.  So here we handle the cash settlement manually,
			// and the next transaction will actually expire (i.e. close) the
			// opening transaction, without recording any P/L.
			p.recordPL(tx, tx.value)
			if !tx.option {
				glog.Fatalf("not an options trade %s: %#v", tx, tx)
			}
			if tx.long {
				p.optionsNotionalSold = p.optionsNotionalSold.Add(tx.value)
			} else {
				p.optionsNotionalBought = p.optionsNotionalBought.Add(tx.value)
			}
			return
		}
		if tx.open && !tx.future && tx.value.Equal(decimal.Zero) {
			glog.Infof("Incoming ACAT without a cost basis in %s: %s on %s",
				tx.underlying, tx, tx.date)
		}
		// Trade caused by an assignment or exercise.
		// We get two entries per position: one to tell us the position
		// expired and was assigned/exercised, and one with the transaction
		// in the underlying equity.
		if tx.action != "" {
			p.handleTrade(tx, count)
			return
		}
	}
	// We can't use closePosition() here because we don't know whether
	// we're closing a long or a short position.  So the best we can
	// do is just close all positions for this symbol.  In theory, if
	// we held both long and short positions for the same option, that
	// would be problematic, in practice however I don't believe that's
	// possible on TW.
	pos, ok := p.positions[tx.symbol]
	if !ok {
		glog.Fatalf("Couldn't find an opening transaction for %s %#v", tx, tx)
	}
	remaining := tx.quantity.Abs() // clone
	for _, open := range pos.opens {
		closed := decimal.Min(remaining, open.qtyOpen)
		premium := open.avgPrice.Mul(closed) // Amount of premium of assigned position
		p.premium = p.premium.Sub(premium)
		tx.rpl = premium
		glog.V(3).Infof("position %s expired by %s ==> realized P&L = %s", open, tx, premium)
		remaining = remaining.Sub(closed)
		// Close off this opening transaction.
		open.qtyOpen = open.qtyOpen.Sub(closed)
		if expired {
			if count {
				p.recordPL(open, premium)
			}
			tx.openTx = open
		} else if tx.openTx != nil {
			// Adjust the cost basis of the underlying position by the amount of
			// premium in the original option position.
			if tx.openTx.open {
				glog.V(3).Infof("Adjusting cost basis of %s by %s", tx.openTx, premium)
				tx.openTx.rpl = premium
			} else {
				glog.V(3).Infof("Adjusting realized P&L of %s by %s", tx.openTx, premium)
				if count {
					p.recordPL(tx.openTx, premium)
				}
			}
		} else {
			// This is kind of a hack: if we couldn't link the two assignment/exercise
			// transactions together, we fall back to recording the P&L of options
			// separately here (rather than trying to adjust the cost basis / P&L in the
			// underlying).  We need this to properly compute P&L when adding transactions
			// incrementally with AddTransaction().
			p.recordPL(open, premium)
		}
	}
	if !remaining.Equal(decimal.Zero) {
		p.synthesizeOpeningTransaction(tx, remaining)
	}
	p.purgeClosed(tx, pos)
}

// Purge closed positions from our portfolio.
func (p *portfolio) purgeClosed(tx *transaction, pos *position) {
	var stillOpen int
	for _, open := range pos.opens {
		if !open.qtyOpen.Equal(decimal.Zero) {
			stillOpen += 1
		}
	}
	if stillOpen == 0 { // No position left for this underlying.
		delete(p.positions, tx.symbol) // Clear it out completely.
	} else {
		// Ditch the opening transactions that are fully closed, keep the rest.
		opens := make([]*transaction, stillOpen)
		var j int
		for _, open := range pos.opens {
			if !open.qtyOpen.Equal(decimal.Zero) {
				opens[j] = open
				j++
			}
		}
		pos.opens = opens
	}
}

func (p *portfolio) handleTransaction(tx *transaction) {
	count := !p.ytd || tx.ytd()
	if count {
		p.comms = p.comms.Add(tx.commission)
		p.fees = p.fees.Add(tx.fees)
	}
	switch tx.txType {
	case "Trade":
		// Increment numTrades here and not in handleTrade because trades
		// caused by option positions assignments/exercises aren't really
		// trades per se.
		p.numTrades++
		fallthrough
	case "Administrative Transfer":
		p.handleTrade(tx, count)
	case "Receive Deliver":
		p.handleAssignmentOrExercise(tx, count)
	case "Money Movement":
		// Handle daily mark-to-market settlement of futures
		if !tx.mtm {
			glog.Fatalf("Unhandled money movement transaction in %s", tx)
		}
		pos, ok := p.positions[tx.symbol]
		if !ok {
			glog.Fatalf("Couldn't find an opening transaction for %s", tx)
		}
		if glog.V(4) {
			descs := make([]string, len(pos.opens))
			for i, open := range pos.opens {
				descs[i] = open.String()
			}
			glog.Infof("%s -> %s ==> mark-to-market = %s",
				strings.Join(descs, ", "), tx, tx.value)
		}
		p.recordPL(tx, tx.value)
	default:
		glog.Fatalf("Unhandled transaction type %q in %s", tx.txType, tx)
	}
}

func (p *portfolio) recordPL(tx *transaction, amount decimal.Decimal) {
	p.rpl = p.rpl.Add(amount)
	rpl, ok := p.rplPerUnderlying[tx.underlying]
	if ok {
		amount = rpl.Add(amount)
	}
	p.rplPerUnderlying[tx.underlying] = amount
}

func (p *portfolio) openPosition(tx *transaction) {
	pos, ok := p.positions[tx.symbol]
	if !ok {
		pos = new(position)
		p.positions[tx.symbol] = pos
	}
	pos.opens = append(pos.opens, tx)
	if tx.option {
		p.premium = p.premium.Add(tx.value)
	}
}

func (p *portfolio) closePosition(tx *transaction, count bool) {
	// Find a matching, opposite transaction(s)
	pos, ok := p.positions[tx.symbol]
	if !ok {
		glog.Fatalf("Couldn't find an opening transaction for %s", tx)
	}
	remaining := tx.quantity.Abs() // Clone
	for _, open := range pos.opens {
		if open.long == tx.long { // Same direction, ignore.
			glog.V(4).Infof("ignore %s -- it's not closed by %s", open, tx)
			continue
		}
		closed := decimal.Min(remaining, open.qtyOpen)
		if tx.option {
			p.premium = p.premium.Sub(open.avgPrice.Mul(closed))
		}
		remaining = remaining.Sub(closed)
		rpl := open.avgPrice.Add(tx.avgPrice).Mul(closed)
		glog.V(4).Infof("%s -> %s [%s+%s] ==> realized P&L = %s",
			open, tx, open.avgPrice, tx.avgPrice, rpl)
		if !open.option && !open.rpl.Equal(decimal.Zero) {
			// Special case for positions opened as a result of assignment/exercise:
			// adjust the P&L by the amount of premium on the original option.
			adj := open.rpl.Div(open.qtyOpen).Mul(closed)
			rpl = rpl.Add(adj)
			glog.V(4).Infof("Adjusted realized P&L by %s to %s", adj, rpl)
		}
		// Close off this opening transaction.
		open.qtyOpen = open.qtyOpen.Sub(closed)
		tx.openTx = open
		tx.rpl = tx.rpl.Add(rpl)
		if count {
			p.recordPL(tx, rpl)
		}
		if remaining.Equal(decimal.Zero) {
			// We closed off enough opening transactions.
			glog.V(3).Infof("done handling %s after closing %s", tx, open)
			break
		} // else: need to close some more opening transactions.
		glog.V(3).Infof("not done handling %s after closing %s", tx, open)
	}
	if !remaining.Equal(decimal.Zero) {
		p.synthesizeOpeningTransaction(tx, remaining)
	}
	p.purgeClosed(tx, pos)
}

// When we close more than what we had open, we need to open the remainder on
// the opposite side of the market (e.g. long 100 shares, sell to close 200 =>
// now short 100).
// This helper synthesizes an opening order for the remaining quantity.
func (p *portfolio) synthesizeOpeningTransaction(tx *transaction, remaining decimal.Decimal) {
	// Only allow spilling over stock/futures at this time.
	if tx.option {
		panic("not supported yet") // TODO: adjust premium maybe?
	}
	otx := *tx               // Copy the original transaction.
	otx.quantity = remaining // Carry over what's left only.
	otx.qtyOpen = remaining
	otx.value = otx.avgPrice.Mul(remaining)
	otx.openTx = tx         // Link synthesized transaction to the closing one.
	otx.fees = decimal.Zero // Synthesized transaction so no fees ..
	var action string
	if otx.long {
		action = "buy"
	} else {
		action = "short"
	}
	otx.action = strings.Replace(otx.action, "CLOSE", "OPEN", 1)
	otx.open = true
	otx.description = fmt.Sprintf("(synthesized) %s to open %s %s @ %s",
		action, remaining, otx.symbol, otx.avgPrice)
	otx.rpl = decimal.Zero
	otx.commission = decimal.Zero // .. and no commissions, since it's made up.
	glog.V(2).Infof("left with %s remaining after closing %s -> synthesized opening %s",
		remaining, tx, &otx)
	p.openPosition(&otx)
}

// Detect rolled options position.  We consider two transactions to make up a
// roll if they fulfill the following conditions:
//   1. They are for the same underlying and same type of option (e.g. both
//      put or call) but opposite sides of the market (one long and one short)
//   2. The two transactions happened within a 30s window.
//   3. The two transactions have the same number of contracts.
//   4. One of the two transactions is a closing transaction, the other is an
//      opening transaction.
func (p *portfolio) detectRoll(tx *transaction) {
	if !tx.option {
		return
	}
	qty := tx.quantity.IntPart()
	if !decimal.New(qty, 0).Equal(tx.quantity) {
		glog.Fatalf("Non-whole number of contracts %s in %s", tx.quantity, tx)
	} else if qty > 255 {
		glog.Fatalf("Really, more than 255 contracts?! %s", tx)
	}
	// Look for an earlier transaction that could make up a roll.
	key := recentKey{
		underlying: tx.underlying, // Look for the same underlying
		call:       tx.call,       // and same type of option (both puts or both calls)
		long:       !tx.long,      // but opposite side of the market
		open:       !tx.open,      // and opening if we're closing or vice versa
	}
	recent, ok := p.recentTx[key]
	if ok { // Look for a matching transaction.
		for i, earlier := range recent {
			if earlier == nil {
				continue
			}
			if earlier.date.After(tx.date) {
				glog.Fatalf("recentTx out of order at time %s (tx=%s)", tx.date, tx)
			} else if t := tx.date.Sub(earlier.date); t > rollWindow {
				glog.V(4).Infof("%s is not a roll of %s as they are %s apart",
					tx, earlier, t)
				continue
			}
			if !tx.open {
				tx, earlier = earlier, tx
			}
			glog.V(2).Infof("detected roll: %s -> %s (P&L: %s)", earlier, tx, earlier.rpl)
			recent[i] = nil // So we don't try to re-use this recent transaction
			tx.rolledFrom = earlier
			return
		}
	}

	// We didn't find a roll, record this transaction in our recent
	// transactions in case there is another one shortly after this one that
	// constitutes a roll when coupled with this one.
	key.long = tx.long
	key.open = tx.open
	recent, ok = p.recentTx[key]
	if !ok {
		p.recentTx[key] = []*transaction{tx}
		return
	}
	p.recentTx[key] = append(recent, tx)
}

func (p *portfolio) PrintPositions() {
	fmt.Println("----- Current portfolio ------")

	// First group all the positions per underlying
	perUnderlying := make(map[string]*position, len(p.positions))
	for _, pos := range p.positions {
		for _, open := range pos.opens {
			underlying := open.underlying
			if open.future {
				// Drop the month and year
				underlying = underlying[:len(underlying)-2]
			}
			pos, ok := perUnderlying[underlying]
			if !ok {
				pos = new(position)
				perUnderlying[underlying] = pos
			}
			pos.opens = append(pos.opens, open)
		}
	}

	// Sort the underlying names
	underlyings := make([]string, len(perUnderlying))
	var i int
	for underlying := range perUnderlying {
		underlyings[i] = underlying
		i++
	}
	sort.Sort(sort.StringSlice(underlyings))

	thisYear := time.Now().Year()
	for _, underlying := range underlyings {
		pos := perUnderlying[underlying]
		var plural string
		if len(pos.opens) > 1 {
			plural = "s"
		}
		var rpl string
		if pl, ok := p.rplPerUnderlying[underlying]; ok {
			var ytd string
			if p.ytd {
				ytd = "YTD "
			}
			rpl = fmt.Sprintf(" [%sRPL=%s]", ytd, pl.StringFixed(2))
		}
		fmt.Printf("%-6s(%d position%s)%s\n", underlying, len(pos.opens), plural, rpl)
		sort.Slice(pos.opens, func(i, j int) bool {
			a := pos.opens[i]
			b := pos.opens[j]
			if a.expDate.Before(b.expDate) {
				return true
			} else if a.expDate.After(b.expDate) {
				return false
			} else if a.strike.LessThan(b.strike) {
				return true
			} else if b.strike.LessThan(a.strike) {
				return false
			}
			// Same exp date & same strike, break tie by premium.
			return a.avgPrice.LessThan(b.avgPrice)
		})
		var prevUsed bool // true if the previous leg was included as part of a multi-leg position.
		for i, open := range pos.opens {
			var long string
			if open.long {
				long = "long "
			} else {
				long = "short"
			}
			var net string
			if !open.option {
				// If this is an equity position that was opened as a result of an
				// exercise or assignment, show the adjusted cost basis.
				if !open.rpl.Equal(decimal.Zero) {
					net = fmt.Sprintf(" (adj. cost basis %s)",
						open.value.Add(open.rpl).Neg().Div(open.quantity).StringFixed(2))
				}
				var kind string
				var openPrice decimal.Decimal
				if open.future {
					kind = "contract"
					openPrice = open.parsePrice()
				} else {
					kind = "share"
					openPrice = open.avgPrice.Abs()
				}
				if open.qtyOpen.GreaterThan(decimal.New(1, 0)) {
					kind += "s"
				}
				fmt.Printf("  %s %s %s @ %s%s\n", long, open.qtyOpen, kind, openPrice.StringFixed(2), net)
				continue
			}
			var expFmt string
			if open.expDate.Year() == thisYear {
				expFmt = "Jan 02"
			} else {
				expFmt = "Jan 02 '06"
			}
			var call string
			if open.call {
				call = "call"
			} else {
				call = "put"
			}
			mult := decimal.New(int64(open.multiplier), 0)
			// Adjust premium amounts per open contract
			perContract := func(v decimal.Decimal) string {
				return v.Div(open.qtyOpen).StringFixed(2)
			}
			var netcr decimal.Decimal // net credit
			if open.rolledFrom != nil {
				netcr = open.NetCredit()
				if netcr.LessThanOrEqual(open.avgPrice) {
					net = fmt.Sprintf(", booked loss %s", perContract(open.avgPrice.Sub(netcr).Div(mult)))
				}
				net = fmt.Sprintf(" (net credit %s%s)", perContract(netcr.Div(mult)), net)
			} else {
				netcr = open.avgPrice
			}
			// Break even point = strike + premium for calls, strike - premium for puts.
			bep := netcr.Abs().Div(mult).Div(open.qtyOpen) // Premium per share
			if !open.call {
				bep = bep.Neg() // For a put we subtract instead
			}
			bep = bep.Add(open.strike)
			fmt.Printf("  %s %s %s $%s %-4s @ %s [BEP=%s]%s\n",
				open.expDate.Format(expFmt), long, open.qtyOpen, open.strike,
				call, open.avgPrice.Div(mult), bep.StringFixed(2), net)

			// Lame-ass attempt to detect two-leg positions.
			if i == 0 {
				continue
			}
			if prevUsed {
				prevUsed = false
				continue
			}
			prev := pos.opens[i-1]
			sameExpiration := prev.expDate.Equal(open.expDate)
			sameQty := prev.qtyOpen.Equal(open.qtyOpen)

			if prev.rolledFrom != nil || open.rolledFrom != nil {
				prevNetCr := prev.NetCredit()
				thisNetCr := open.NetCredit()
				netcr = prevNetCr.Add(thisNetCr)
			} else {
				netcr = prev.value.Add(open.value)
			}
			credit := "credit"
			if netcr.LessThan(decimal.Zero) {
				credit = "debit"
			}

			// We know positions are sorted by expiration and then by strike so if
			// we have a spread or strangle or straddle it's very likely that the
			// previous position is the other leg of this two-leg position.
			// This approach is very hackish and not reliable but should work mostly
			// fine for simpler portfolios for now.
			if prev.long != open.long && prev.call == open.call && sameExpiration && sameQty {
				fmt.Printf("  --> %s/%s %s spread @ %s net %s\n",
					prev.strike, open.strike, call, perContract(netcr.Div(mult)), credit)
				prevUsed = true
			} else if prev.long == open.long && prev.call != open.call && sameExpiration && sameQty {
				// Strangle or straddle
				var position string
				if prev.strike.Equal(open.strike) {
					position = fmt.Sprintf("%s straddle", open.strike)
				} else {
					if prev.call {
						prev, open = open, prev // always list the put before the call
					}
					var inverted string
					if prev.strike.GreaterThan(open.strike) {
						inverted = fmt.Sprintf("inverted [%s wide] ", prev.strike.Sub(open.strike))
					}
					position = fmt.Sprintf("%s/%s %sstrangle", prev.strike, open.strike, inverted)
				}
				fmt.Printf("  --> %s @ %s net %s\n",
					position, perContract(netcr.Div(mult)), credit)
				prevUsed = true
			}
		}
	}
}

func (p *portfolio) PrintStats() {
	if p.ytd {
		fmt.Println("------- YTD statistics -------")
	} else {
		fmt.Println("----- Overall statistics -----")
	}
	if len(p.transactions) < 2 {
		fmt.Println("Not enough transactions yet")
		return
	}
	const day = 24 * time.Hour
	numTrades := p.numTrades
	var beginTime time.Time
	if p.ytd {
		beginTime = ytdStart
		var n uint16
		for _, tx := range p.transactions {
			if tx.ytd() { // Once we find the first YTD transaction...
				numTrades -= n // ... discount all the ones before.
				break
			} else if tx.txType == "Trade" {
				n++
			}
		}
	} else {
		beginTime = p.transactions[0].date
	}
	duration := p.transactions[len(p.transactions)-1].date.Sub(beginTime).Round(day)
	days := int(duration / day)
	if days == 0 {
		days = 1 // Simplify division by zero handling by saying we have at least one day.
	}
	pct := func(amount decimal.Decimal) string {
		if p.rpl.Equal(decimal.Zero) {
			return "0%"
		}
		return amount.Mul(oneHundred).Div(p.rpl).StringFixed(2) + "%"
	}
	fmt.Printf("Number of transactions: %6d    (in %d days => %.1f/day avg)\n",
		numTrades, days, float32(numTrades)/float32(days))
	fmt.Printf("Realized P&L:           %9s\n", p.rpl.StringFixed(2))
	fmt.Printf("Commissions:            %9s (%s of P&L)\n", p.comms.StringFixed(2), pct(p.comms.Neg()))
	fmt.Printf("Fees:                   %9s (%s of P&L)\n", p.fees.StringFixed(2), pct(p.fees.Neg()))
	fmt.Printf("Interest:               %9s (%s of P&L)\n", p.intrs.StringFixed(2), pct(p.intrs.Neg()))
	grosspl := p.rpl.Add(p.comms).Add(p.fees).Add(p.intrs)
	fmt.Printf("Gross P&L:              %9s (~%s/day avg, %s of P&L)\n",
		grosspl.StringFixed(2),
		grosspl.Div(decimal.New(int64(days), 0)).StringFixed(2), pct(grosspl))
	var premium decimal.Decimal
	var neq uint                       // Number of equity positions
	var equity decimal.Decimal         // Cost of equity
	var equityDiscount decimal.Decimal // Adj. cost basis of equity acquired from options
	for _, pos := range p.positions {
		for _, open := range pos.opens {
			if open.option {
				premium = premium.Add(open.avgPrice.Mul(open.qtyOpen))
			} else if !open.future {
				// Adjust for the number of shares actually still open
				scale := open.qtyOpen.Div(open.quantity)
				equity = equity.Add(open.value).Mul(scale)
				equityDiscount = equityDiscount.Add(open.rpl).Mul(scale)
				neq++
			}
		}
	}
	fmt.Printf("Equity:                 %9s (%d positions)\n", equity.StringFixed(2), neq)
	fmt.Printf("Adjusted Gross P&L:     %9s\n", grosspl.Add(equity).StringFixed(2))
	fmt.Printf("Net money movements:    %9s\n", p.moneyMov.StringFixed(2))
	fmt.Printf("Outstanding premium:    %9s\n", p.premium.StringFixed(2))
	if !premium.Equal(p.premium) {
		fmt.Printf("-> Warning: estimated outstanding premium should've been %s (difference: %s)\n",
			premium.StringFixed(2), premium.Sub(p.premium).StringFixed(2))
	}
	fmt.Printf("Cash on hand:           %9s\n", p.cash.StringFixed(2))
	if !p.ytd {
		// Alternative method to compute cash on hand
		cash := grosspl.Add(premium).Add(p.moneyMov).Add(equity).Add(equityDiscount).Add(p.miscCash)
		if !cash.Equal(p.cash) {
			fmt.Printf("-> Warning: estimated cash on hand should've been %s (difference: %s)\n",
				cash.StringFixed(2), cash.Sub(p.cash).StringFixed(2))
		}
	}
}

func (p *portfolio) PrintCumulativeStats() {
	fmt.Println("------ Cumulative stats ------")
	fmt.Printf("Contracts traded:       %6d\n", p.putsTraded+p.callsTraded)
	fmt.Printf("  Puts traded:          %6d\n", p.putsTraded)
	fmt.Printf("  Calls traded:         %6d\n", p.callsTraded)
	fmt.Printf("Notional sold:        %11s\n", p.optionsNotionalSold.StringFixed(2))
	fmt.Printf("Notional bought:      %11s\n", p.optionsNotionalBought.StringFixed(2))
	fmt.Printf("  Difference:         %11s\n", p.optionsNotionalSold.Add(p.optionsNotionalBought).StringFixed(2))
	fmt.Printf("Equities sold:        %11s\n", p.equityNotionalSold.StringFixed(2))
	fmt.Printf("Equities bought:      %11s\n", p.equityNotionalBought.StringFixed(2))
	fmt.Printf("  Difference:         %11s\n", p.equityNotionalSold.Add(p.equityNotionalBought).StringFixed(2))
	var notionalOpen decimal.Decimal
	for _, pos := range p.positions {
		for _, open := range pos.opens {
			if open.future && !open.option {
				notionalOpen = notionalOpen.Add(futuresNotional(open))
			}
		}
	}
	fmt.Printf("Futures sold:       %13s\n", p.futuresNotionalSold.StringFixed(2))
	fmt.Printf("Futures bought:     %13s\n", p.futuresNotionalBought.StringFixed(2))
	diff := p.futuresNotionalSold.Add(p.futuresNotionalBought)
	fmt.Printf("  Difference:       %13s\n", diff.StringFixed(2))
	if !notionalOpen.Equal(decimal.Zero) {
		fmt.Printf("  Open:             %13s\n", notionalOpen.StringFixed(2))
		fmt.Printf("  Net:              %13s\n", diff.Sub(notionalOpen).StringFixed(2))
	}
}

func (p *portfolio) PrintPL() {
	fmt.Println("---- Realized P&L detail -----")
	underlyings := make([]string, len(p.rplPerUnderlying))
	var i int
	for underlying := range p.rplPerUnderlying {
		underlyings[i] = underlying
		i++
	}
	sort.Sort(sort.StringSlice(underlyings))
	for _, underlying := range underlyings {
		fmt.Printf("%-6s%9s\n", underlying, p.rplPerUnderlying[underlying].StringFixed(2))
	}
}

func dumpChart(records [][]string, ytd, nofutures, ignoreacat bool) {
	// We don't really have a good way to track stats step by step, so rebuild the
	// portfolio by adding the transactions one by one for now.
	portfolio := NewPortfolio(records[1:2], ytd, nofutures, ignoreacat) // Just the first transaction
	var xv []time.Time
	var rpl []float64
	var adjRpl []float64
	var premium []float64
	var cash []float64
	records = records[2:]
	for i, record := range records {
		//fmt.Println("----------------------------------->", record)
		if nofutures && record[4] == "Future" {
			continue
		}
		portfolio.AddTransaction(record)
		//portfolio.PrintStats()
		date, err := time.Parse(almostRFC3339, record[0])
		if err != nil {
			glog.Fatalf("record #%d, bad transaction date: %s", len(portfolio.transactions), err)
		}
		// If we have multiple transactions at exactly the same time (common with rolls
		// and options expiration etc), keep processing transactions until we have a
		// time change so that we have only one data point per timestamp.
		if i != len(records)-1 {
			nextDate, err := time.Parse(almostRFC3339, records[i+1][0])
			if err == nil && date.Equal(nextDate) {
				continue
			}
		}
		amount, _ := portfolio.rpl.Float64()
		xv = append(xv, date)
		rpl = append(rpl, amount)
		grosspl := portfolio.rpl.Add(portfolio.comms).Add(portfolio.fees).Add(portfolio.intrs)
		amount, _ = grosspl.Float64()
		adjRpl = append(adjRpl, amount)
		amount, _ = portfolio.premium.Float64()
		premium = append(premium, amount)
		amount, _ = portfolio.cash.Float64()
		cash = append(cash, amount)
	}

	graph := chart.Chart{
		Width:  1400,
		Height: 700,
		XAxis: chart.XAxis{
			Style:          chart.StyleShow(),
			TickPosition:   chart.TickPositionUnderTick,
			ValueFormatter: chart.TimeValueFormatterWithFormat("Jan 2 '06"),
			Range: &chart.MarketHoursRange{
				MarketOpen:      util.NYSEOpen(),
				MarketClose:     util.NYSEClose(),
				HolidayProvider: util.Date.IsNYSEHoliday,
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorLightGray,
				StrokeWidth: 1.0,
			},
			GridMinorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorLightGray,
				StrokeWidth: 1.0,
			},
		},
		YAxis: chart.YAxis{
			Style: chart.StyleShow(),
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorLightGray,
				StrokeWidth: 1.0,
			},
			GridMinorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorLightGray,
				StrokeWidth: 1.0,
			},
		},
		Series: []chart.Series{
			chart.TimeSeries{
				Name:    "Realized P&L",
				XValues: xv,
				YValues: rpl,
				Style: chart.Style{
					StrokeDashArray: []float64{3.0, 3.0},
				},
			},
			chart.TimeSeries{
				Name:    "Adjusted Realized P&L",
				XValues: xv,
				YValues: adjRpl,
			},
			chart.TimeSeries{
				Name:    "Outstanding Premium",
				XValues: xv,
				YValues: premium,
			},
			chart.TimeSeries{
				Name:    "Cash on hand",
				XValues: xv,
				YValues: cash,
			},
		},
	}
	graph.Elements = []chart.Renderable{
		chart.Legend(&graph),
	}

	rplPng, err := os.Create("rpl.png")
	if err != nil {
		glog.Fatal(err)
	}
	defer rplPng.Close()
	graph.Render(chart.PNG, rplPng)
}

func main() {
	input := flag.String("input", "", "input csv file containing tastyworks transactions")
	stats := flag.Bool("stats", true, "print overall statistics")
	cumulative := flag.Bool("cumulative", true, "print cumulative statistics")
	ytd := flag.Bool("ytd", false, "limit output to YTD transactions")
	printPL := flag.Bool("printpl", false, "print realized P&L per underlying")
	positions := flag.Bool("positions", false, "print current positions")
	chart := flag.Bool("chart", false, "create a chart of P&L")
	nofutures := flag.Bool("nofutures", false, "ignore all futures transactions")
	ignoreacat := flag.Bool("ignoreacat", false, "ignore all ACAT transfers")
	flag.Parse()
	if *input == "" {
		glog.Fatal("-input flag required")
	}
	records := loadCSV(*input)
	if len(records) < 3 { // need header + 2 transactions minimum
		glog.Fatal("not enough records, check CSV file")
	}
	if records[0][0] != "Date" { // Quick sanity check
		glog.Fatal("CSV seems malformed")
	}

	portfolio := NewPortfolio(records[1:], *ytd, *nofutures, *ignoreacat)
	if *stats {
		portfolio.PrintStats()
	}
	if *cumulative {
		portfolio.PrintCumulativeStats()
	}
	if *printPL {
		portfolio.PrintPL()
	}
	if *chart {
		dumpChart(records, *ytd, *nofutures, *ignoreacat)
	}
	if *positions {
		portfolio.PrintPositions()
	}
}
