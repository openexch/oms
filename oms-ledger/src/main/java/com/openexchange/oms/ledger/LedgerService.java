package com.openexchange.oms.ledger;

import com.match.domain.FixedPoint;
import com.openexchange.oms.common.domain.Market;
import com.openexchange.oms.common.domain.OmsOrder;
import com.openexchange.oms.common.domain.SnowflakeIdGenerator;
import com.openexchange.oms.common.enums.LedgerEntryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Orchestrates ledger operations, coordinating between the {@link BalanceStore} and
 * the double-entry accounting log ({@link LedgerEntry}).
 * <p>
 * Each operation mutates balances in the store and returns a list of {@link LedgerEntry}
 * records for the persistence layer to batch-write to the journal.
 */
public class LedgerService {

    private static final Logger log = LoggerFactory.getLogger(LedgerService.class);

    private final BalanceStore balanceStore;
    private final SnowflakeIdGenerator idGenerator;

    public LedgerService(BalanceStore balanceStore, SnowflakeIdGenerator idGenerator) {
        this.balanceStore = balanceStore;
        this.idGenerator = idGenerator;
    }

    /**
     * Holds funds for a new order.
     * <p>
     * Buy orders hold quote asset: {@code price * quantity}.
     * Sell orders hold base asset: {@code quantity}.
     *
     * @param order the order to hold funds for
     * @return ledger entries (debit available, credit locked), or empty list if hold failed
     */
    public List<LedgerEntry> holdForOrder(OmsOrder order) {
        Market market = Market.fromId(order.getMarketId());
        int holdAssetId;
        long holdAmount;

        if (order.isBuy()) {
            // Buy: hold quote asset (price * quantity)
            holdAssetId = market.quoteAsset().id();
            holdAmount = FixedPoint.multiply(order.getPrice(), order.getQuantity());
        } else {
            // Sell: hold base asset (quantity)
            holdAssetId = market.baseAsset().id();
            holdAmount = order.getQuantity();
        }

        if (holdAmount <= 0) {
            log.error("Calculated hold amount is non-positive: orderId={}, holdAmount={}",
                    order.getOmsOrderId(), holdAmount);
            return Collections.emptyList();
        }

        boolean success = balanceStore.hold(order.getUserId(), holdAssetId, holdAmount, order.getOmsOrderId());
        if (!success) {
            log.warn("Hold failed for order: orderId={}, userId={}, assetId={}, amount={}",
                    order.getOmsOrderId(), order.getUserId(), holdAssetId, holdAmount);
            return Collections.emptyList();
        }

        // Update order with hold metadata
        order.setHoldAmount(holdAmount);

        long now = System.currentTimeMillis();
        long journalId = idGenerator.nextId();

        List<LedgerEntry> entries = new ArrayList<>(2);

        // Debit: decrease available
        entries.add(new LedgerEntry(
                idGenerator.nextId(), journalId,
                order.getUserId(), holdAssetId, holdAmount,
                LedgerEntryType.ORDER_HOLD, true,
                order.getOmsOrderId(), now));

        // Credit: increase locked
        entries.add(new LedgerEntry(
                idGenerator.nextId(), journalId,
                order.getUserId(), holdAssetId, holdAmount,
                LedgerEntryType.ORDER_HOLD, false,
                order.getOmsOrderId(), now));

        log.debug("Hold created: orderId={}, assetId={}, amount={}", order.getOmsOrderId(), holdAssetId, holdAmount);
        return entries;
    }

    /**
     * Releases remaining held funds when an order is cancelled.
     * <p>
     * The release amount is computed as the hold amount minus what was already filled.
     * For buy orders: {@code holdAmount - (filledQty * price)}.
     * For sell orders: {@code remainingQty}.
     *
     * @param order the cancelled order
     * @return ledger entries (debit locked, credit available), or empty list if nothing to release
     */
    public List<LedgerEntry> releaseForCancel(OmsOrder order) {
        Market market = Market.fromId(order.getMarketId());
        int holdAssetId;
        long releaseAmount;

        if (order.isBuy()) {
            // Buy: release remaining quote hold
            // holdAmount was price * originalQty; already consumed = price * filledQty
            holdAssetId = market.quoteAsset().id();
            long consumed = FixedPoint.multiply(order.getPrice(), order.getFilledQty());
            releaseAmount = order.getHoldAmount() - consumed;
        } else {
            // Sell: release remaining base quantity
            holdAssetId = market.baseAsset().id();
            releaseAmount = order.getRemainingQty();
        }

        if (releaseAmount <= 0) {
            log.debug("Nothing to release for order: orderId={}", order.getOmsOrderId());
            return Collections.emptyList();
        }

        boolean success = balanceStore.release(order.getUserId(), holdAssetId, releaseAmount, order.getOmsOrderId());
        if (!success) {
            log.error("Release failed for order: orderId={}, userId={}, assetId={}, amount={}",
                    order.getOmsOrderId(), order.getUserId(), holdAssetId, releaseAmount);
            return Collections.emptyList();
        }

        long now = System.currentTimeMillis();
        long journalId = idGenerator.nextId();

        List<LedgerEntry> entries = new ArrayList<>(2);

        // Debit: decrease locked
        entries.add(new LedgerEntry(
                idGenerator.nextId(), journalId,
                order.getUserId(), holdAssetId, releaseAmount,
                LedgerEntryType.ORDER_RELEASE, true,
                order.getOmsOrderId(), now));

        // Credit: increase available
        entries.add(new LedgerEntry(
                idGenerator.nextId(), journalId,
                order.getUserId(), holdAssetId, releaseAmount,
                LedgerEntryType.ORDER_RELEASE, false,
                order.getOmsOrderId(), now));

        log.debug("Release completed: orderId={}, assetId={}, amount={}", order.getOmsOrderId(), holdAssetId, releaseAmount);
        return entries;
    }

    /**
     * Settles a trade execution, moving funds between buyer and seller.
     * <p>
     * Buyer: locked quote decreases by {@code price * quantity}, available base increases by {@code quantity}.
     * Seller: locked base decreases by {@code quantity}, available quote increases by {@code price * quantity}.
     *
     * @param tradeId          unique trade identifier
     * @param buyerUserId      buyer's user ID
     * @param sellerUserId     seller's user ID
     * @param marketId         market identifier
     * @param price            execution price (fixed-point)
     * @param quantity         execution quantity (fixed-point)
     * @param takerOmsOrderId  taker's OMS order ID
     * @param makerOmsOrderId  maker's OMS order ID
     * @return ledger entries for both sides of the trade
     */
    public List<LedgerEntry> settleTradeExecution(long tradeId, long buyerUserId, long sellerUserId,
                                                   int marketId, long price, long quantity,
                                                   long takerOmsOrderId, long makerOmsOrderId) {
        Market market = Market.fromId(marketId);
        int baseAssetId = market.baseAsset().id();
        int quoteAssetId = market.quoteAsset().id();

        long baseAmount = quantity;
        long quoteAmount = FixedPoint.multiply(price, quantity);

        if (baseAmount <= 0 || quoteAmount <= 0) {
            log.error("Invalid trade amounts: tradeId={}, baseAmount={}, quoteAmount={}",
                    tradeId, baseAmount, quoteAmount);
            return Collections.emptyList();
        }

        balanceStore.settle(buyerUserId, sellerUserId, baseAssetId, quoteAssetId, baseAmount, quoteAmount, tradeId);

        long now = System.currentTimeMillis();
        List<LedgerEntry> entries = new ArrayList<>(4);

        // --- Buyer side ---
        long buyerJournalId = idGenerator.nextId();

        // Buyer debit: locked quote decreases (trade debit on quote asset)
        entries.add(new LedgerEntry(
                idGenerator.nextId(), buyerJournalId,
                buyerUserId, quoteAssetId, quoteAmount,
                LedgerEntryType.TRADE_DEBIT, true,
                tradeId, now));

        // Buyer credit: available base increases (trade credit on base asset)
        entries.add(new LedgerEntry(
                idGenerator.nextId(), buyerJournalId,
                buyerUserId, baseAssetId, baseAmount,
                LedgerEntryType.TRADE_CREDIT, false,
                tradeId, now));

        // --- Seller side ---
        long sellerJournalId = idGenerator.nextId();

        // Seller debit: locked base decreases (trade debit on base asset)
        entries.add(new LedgerEntry(
                idGenerator.nextId(), sellerJournalId,
                sellerUserId, baseAssetId, baseAmount,
                LedgerEntryType.TRADE_DEBIT, true,
                tradeId, now));

        // Seller credit: available quote increases (trade credit on quote asset)
        entries.add(new LedgerEntry(
                idGenerator.nextId(), sellerJournalId,
                sellerUserId, quoteAssetId, quoteAmount,
                LedgerEntryType.TRADE_CREDIT, false,
                tradeId, now));

        log.debug("Trade settled: tradeId={}, buyer={}, seller={}, market={}, price={}, qty={}",
                tradeId, buyerUserId, sellerUserId, marketId, price, quantity);
        return entries;
    }

    /**
     * Releases excess hold when a buy order fills at a price lower than the hold price.
     * <p>
     * The overlock amount is {@code (holdPrice - fillPrice) * fillQty}, representing quote asset
     * that was locked but not needed for the fill.
     *
     * @param omsOrderId OMS order ID
     * @param holdPrice  the price at which funds were originally held (fixed-point)
     * @param fillPrice  the actual execution price (fixed-point)
     * @param fillQty    the fill quantity (fixed-point)
     * @return ledger entries for the release, or empty list if no overlock or release failed
     */
    public List<LedgerEntry> handleOverlock(long omsOrderId, long userId, int marketId,
                                             long holdPrice, long fillPrice, long fillQty) {
        if (fillPrice >= holdPrice) {
            // No overlock: filled at or above hold price
            return Collections.emptyList();
        }

        Market market = Market.fromId(marketId);
        int quoteAssetId = market.quoteAsset().id();

        long priceDelta = holdPrice - fillPrice;
        long overlockAmount = FixedPoint.multiply(priceDelta, fillQty);

        if (overlockAmount <= 0) {
            return Collections.emptyList();
        }

        boolean success = balanceStore.release(userId, quoteAssetId, overlockAmount, omsOrderId);
        if (!success) {
            log.error("Overlock release failed: omsOrderId={}, userId={}, quoteAssetId={}, amount={}",
                    omsOrderId, userId, quoteAssetId, overlockAmount);
            return Collections.emptyList();
        }

        long now = System.currentTimeMillis();
        long journalId = idGenerator.nextId();

        List<LedgerEntry> entries = new ArrayList<>(2);

        // Debit: decrease locked
        entries.add(new LedgerEntry(
                idGenerator.nextId(), journalId,
                userId, quoteAssetId, overlockAmount,
                LedgerEntryType.ORDER_RELEASE, true,
                omsOrderId, now));

        // Credit: increase available
        entries.add(new LedgerEntry(
                idGenerator.nextId(), journalId,
                userId, quoteAssetId, overlockAmount,
                LedgerEntryType.ORDER_RELEASE, false,
                omsOrderId, now));

        log.debug("Overlock released: omsOrderId={}, holdPrice={}, fillPrice={}, fillQty={}, released={}",
                omsOrderId, holdPrice, fillPrice, fillQty, overlockAmount);
        return entries;
    }
}
