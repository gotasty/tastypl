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
)

func (t *transaction) String() string {
	return t.description
}

func (t *transaction) NetCredit() decimal.Decimal {
	net := t.value
	earlier := t.rolledFrom
	for earlier != nil {
		net = net.Add(earlier.rpl)
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
		// Might not be true for options on futures (?)
		glog.Fatalf("options position can't be marked-to-market in %s", t)
	} else if t.multiplier == 0 {
		glog.Fatalf("options position must have a non-zero multiplier in %s", t)
	}
	if t.strike.LessThanOrEqual(decimal.Zero) {
		glog.Fatalf("strike price can't be less than or equal to zero in %s", t)
	}
	if t.date.After(t.expDate) {
		glog.Fatalf("transaction date %s happened after expiration %s in %s",
			t.date, t.expDate, t)
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
		// PM-settled index options have an additional "P" in the symbol.
		// This is a hack to accept those.
		pm := t.underlying + "P"
		symbol = fmt.Sprintf(symfmt, pm, expDate, cp, strike)
		if symbol != t.symbol {
			glog.Fatalf("expected symbol %q but found %q in %s", symbol, t.symbol, t)
		}
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

	// All transactions.
	transactions []*transaction

	// Recent transactions, used to detect rolls of options positions.
	// Transactions older than rollWindow should get purged.
	recentTx map[recentKey][]*transaction

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

	// Summary of realized P&L per underlying
	rplPerUnderlying map[string]decimal.Decimal

	numTrades uint16 // Number of trades executed
}

func NewPortfolio(records [][]string, ytd bool, nofutures bool) *portfolio {
	p := &portfolio{
		ytd:              ytd,
		positions:        make(map[string]*position),
		recentTx:         make(map[recentKey][]*transaction),
		rplPerUnderlying: make(map[string]decimal.Decimal),
	}

	// The CSV is sorted from newest to oldest transaction, reverse it.
	for i := len(records)/2 - 1; i >= 0; i-- {
		opp := len(records) - 1 - i
		records[i], records[opp] = records[opp], records[i]
	}
	p.parseTransactions(records, nofutures)

	var prevTime time.Time
	for i, tx := range p.transactions {
		if prevTime.After(tx.date) {
			glog.Fatalf("transaction log out of order at time %s (tx=%s)", tx.date, tx)
		} else if tx.date.Sub(prevTime) > rollWindow && len(p.recentTx) > 0 {
			// More than rollWindow time has elapsed between two transactions,
			// clear our map of recent transactions as we can't possibly find
			// any rolls in it anymore.
			p.recentTx = make(map[recentKey][]*transaction)
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
		default:
			if strings.HasPrefix(record[5], "FROM ") {
				// Interest paid, e.g. "FROM 10/16 THRU 11/15 @ 8    %"
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
			if nofutures && tx.instrument == "Future" {
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
	if comm.GreaterThan(decimal.Zero) {
		glog.Fatalf("record #%d, positive commission amount %s", i, rec[9])
	}
	fees := parseDecimal(rec[10])
	if fees.GreaterThan(decimal.Zero) {
		glog.Fatalf("record #%d, positive fees amount %s", i, rec[10])
	}
	p.cash = p.cash.Add(value).Add(comm).Add(fees)
	txType := rec[1]
	instrument := rec[4]
	var call bool
	var mtm bool
	option := true
	switch rec[15] {
	case "PUT":
	case "CALL":
		call = true
	case "":
		// Handle non-trade transactions.
		if txType != "Trade" && txType != "Receive Deliver" {
			// Detect daily mark-to-market of futures held overnight
			if txType == "Money Movement" && instrument == "Future" &&
				strings.HasSuffix(rec[5], "Final settlement price") {
				mtm = true
			} else {
				p.parseNonOptionTransaction(rec)
				return nil
			}
		}
		if strings.HasSuffix(instrument, "Option") {
			glog.Fatal("WTF, record #%d should be a non-option transaction: %q", i, rec)
		}
		// else: fallthrough (this is a trade but a non-option transaction)
		option = false
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
	return tx
}

func (p *portfolio) handleTrade(tx *transaction, count bool) {
	switch tx.action {
	case "SELL_TO_OPEN", "BUY_TO_OPEN":
		p.openPosition(tx)
		p.detectRoll(tx)
	case "SELL_TO_CLOSE", "BUY_TO_CLOSE":
		p.closePosition(tx, count)
		p.detectRoll(tx)
	default:
		if tx.instrument == "Future" {
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
		glog.Fatalf("Couldn't find an opening transaction for %s", tx)
	}
	quantity := tx.quantity.Abs() // clone
	for _, open := range pos.opens {
		glog.V(3).Infof("position %s expired by %s", open, tx)
		p.premium = p.premium.Sub(open.value)
		tx.rpl = open.value
		quantity = quantity.Sub(open.quantity)
		if expired {
			if count {
				p.recordPL(open, open.value)
			}
			tx.openTx = open
		} else if tx.openTx != nil {
			// Adjust the cost basis of the underlying position by the amount of
			// premium in the original option position.
			if tx.openTx.open {
				glog.V(3).Infof("Adjusting cost basis of %s by %s", tx.openTx, open.value)
				tx.openTx.rpl = open.value
			} else {
				glog.V(3).Infof("Adjusting realized P&L of %s by %s", tx.openTx, open.value)
				if count {
					p.recordPL(tx.openTx, open.value)
				}
			}
		} else {
			// This is kind of a hack: if we couldn't link the two assignment/exercise
			// transactions together, we fall back to recording the P&L of options
			// separately here (rather than trying to adjust the cost basis / P&L in the
			// underlying).  We need this to properly compute P&L when adding transactions
			// incrementally with AddTransaction().
			p.recordPL(open, open.value)
		}
	}
	if !quantity.Equal(decimal.Zero) {
		glog.Fatalf("Left with %d position after handling %s", quantity, tx)
	}
	delete(p.positions, tx.symbol)
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
		// Close off this opening transaction.
		open.qtyOpen = open.qtyOpen.Sub(closed)
		rpl := open.avgPrice.Add(tx.avgPrice).Mul(closed)
		glog.V(4).Infof("%s -> %s [%s+%s] ==> realized P&L = %s",
			open, tx, open.avgPrice, tx.avgPrice, rpl)
		if !open.option && !open.rpl.Equal(decimal.Zero) {
			// Special case for positions opened as a result of assignment/exercise:
			// adjust the P&L by the amount of premium on the original option.
			rpl = rpl.Add(open.rpl)
			glog.V(4).Infof("Adjusted realized P&L by %s to %s", open.rpl, rpl)
		}
		tx.openTx = open
		tx.rpl = rpl
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
		glog.Fatalf("couldn't close %s contracts in %s", remaining, tx)
	}
	// Purge closed positions.
	var stillOpen int
	for _, open := range pos.opens {
		if !open.qtyOpen.Equal(decimal.Zero) {
			stillOpen += 1
		}
	}
	if stillOpen == 0 {
		delete(p.positions, tx.symbol)
	} else {
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
	fmt.Println("----- Current portfolio -----")

	// First group all the positions per underlying
	perUnderlying := make(map[string]*position, len(p.positions))
	for _, pos := range p.positions {
		for _, open := range pos.opens {
			underlying := open.underlying
			if open.instrument == "Future" {
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
			}
			// Same exp date, break tie by strike.
			return a.strike.LessThan(b.strike)
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
				fmt.Printf("  %s %s shares @ %s%s\n", long, open.qtyOpen, open.avgPrice.Abs().StringFixed(2), net)
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
				if netcr.LessThanOrEqual(open.value) {
					net = fmt.Sprintf(", booked loss %s", perContract(open.value.Sub(netcr).Div(mult)))
				}
				net = fmt.Sprintf(" (net credit %s%s)", perContract(netcr.Div(mult)), net)
			} else {
				netcr = open.value
			}
			// Break even point = strike + premium for calls, strike - premium for puts.
			bep := netcr.Abs().Div(mult).Div(open.qtyOpen) // Premium per share
			if !open.call {
				bep = bep.Neg() // For a put we subtract instead
			}
			bep = bep.Add(open.strike)
			fmt.Printf("  %s %s %s $%s %-4s @ %s [BEP=%s] %s\n",
				open.expDate.Format(expFmt), long, open.qtyOpen, open.strike,
				call, perContract(open.value.Div(mult)), bep.StringFixed(2), net)

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
		fmt.Println("------- YTD statistics ------")
	} else {
		fmt.Println("----- Overall statistics ----")
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
	fmt.Printf("Number of transactions: %5d    (in %d days => %.1f/day avg)\n",
		numTrades, days, float32(numTrades)/float32(days))
	fmt.Printf("Realized P&L:           %8s\n", p.rpl.StringFixed(2))
	fmt.Printf("Commissions:            %8s (%s of P&L)\n", p.comms.StringFixed(2), pct(p.comms))
	fmt.Printf("Fees:                   %8s (%s of P&L)\n", p.fees.StringFixed(2), pct(p.fees))
	fmt.Printf("Interest:               %8s (%s of P&L)\n", p.intrs.StringFixed(2), pct(p.intrs))
	grosspl := p.rpl.Add(p.comms).Add(p.fees).Add(p.intrs)
	fmt.Printf("Gross P&L:              %8s (~%s/day avg, %s of P&L)\n",
		grosspl.StringFixed(2),
		grosspl.Div(decimal.New(int64(days), 0)).StringFixed(2), pct(grosspl))
	var premium decimal.Decimal
	var neq uint                       // Number of equity positions
	var equity decimal.Decimal         // Cost of equity
	var equityDiscount decimal.Decimal // Adj. cost basis of equity acquired from options
	for _, pos := range p.positions {
		for _, open := range pos.opens {
			if open.option {
				premium = premium.Add(open.value)
			} else if open.instrument != "Future" {
				equity = equity.Add(open.value)
				equityDiscount = equityDiscount.Add(open.rpl)
				neq++
			}
		}
	}
	fmt.Printf("Equity:                 %8s (%d positions)\n", equity.StringFixed(2), neq)
	fmt.Printf("Adjusted Gross P&L:     %8s\n", grosspl.Add(equity).StringFixed(2))
	fmt.Printf("Net money movements:    %8s\n", p.moneyMov.StringFixed(2))
	fmt.Printf("Outstanding premium:    %8s\n", p.premium.StringFixed(2))
	if !premium.Equal(p.premium) {
		fmt.Printf("-> Warning: estimated outstanding premium should've been %s (difference: %s)\n",
			premium.StringFixed(2), premium.Sub(p.premium).StringFixed(2))
	}
	fmt.Printf("Cash on hand:           %8s\n", p.cash.StringFixed(2))
	if !p.ytd {
		// Alternative method to compute cash on hand
		cash := grosspl.Add(premium).Add(p.moneyMov).Add(equity).Add(equityDiscount).Add(p.miscCash)
		if !cash.Equal(p.cash) {
			fmt.Printf("-> Warning: estimated cash on hand should've been %s (difference: %s)\n",
				cash.StringFixed(2), cash.Sub(p.cash).StringFixed(2))
		}
	}
}

func (p *portfolio) PrintPL() {
	fmt.Println("---- Realized P&L detail ----")
	underlyings := make([]string, len(p.rplPerUnderlying))
	var i int
	for underlying := range p.rplPerUnderlying {
		underlyings[i] = underlying
		i++
	}
	sort.Sort(sort.StringSlice(underlyings))
	for _, underlying := range underlyings {
		fmt.Printf("%-6s%8s\n", underlying, p.rplPerUnderlying[underlying].StringFixed(2))
	}
}

func dumpChart(records [][]string, ytd bool, nofutures bool) {
	// We don't really have a good way to track stats step by step, so rebuild the
	// portfolio by adding the transactions one by one for now.
	portfolio := NewPortfolio(records[1:2], ytd, nofutures) // Just the first transaction
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

	rplPng, err := os.Create("/tmp/rpl.png")
	if err != nil {
		glog.Fatal(err)
	}
	defer rplPng.Close()
	graph.Render(chart.PNG, rplPng)
}

func main() {
	input := flag.String("input", "", "input csv file containing tastyworks transactions")
	stats := flag.Bool("stats", true, "print overall statistics")
	ytd := flag.Bool("ytd", false, "limit output to YTD transactions")
	printPL := flag.Bool("printpl", false, "print realized P&L per underlying")
	positions := flag.Bool("positions", false, "print current positions")
	chart := flag.Bool("chart", false, "create a chart of P&L")
	nofutures := flag.Bool("nofutures", false, "ignore all futures transactions")
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

	portfolio := NewPortfolio(records[1:], *ytd, *nofutures)
	if *stats {
		portfolio.PrintStats()
	}
	if *printPL {
		portfolio.PrintPL()
	}
	if *chart {
		dumpChart(records, *ytd, *nofutures)
	}
	if *positions {
		portfolio.PrintPositions()
	}
}
