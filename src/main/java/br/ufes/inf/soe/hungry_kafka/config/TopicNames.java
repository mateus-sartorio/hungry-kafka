package br.ufes.inf.soe.hungry_kafka.config;

public final class TopicNames {
    public static final String ITEM_VIEW_EVENTS = "item-view-events";
    public static final String CART_EVENTS = "cart-events";

    public static final String ORDER_STATUS_EVENTS = "update-order-status-events";
    public static final String ORDER_EVENTS = "created-order-events";

    public static final String HOT_ITEM_EVENTS = "hot-item-events";
    public static final String LEAD_ITEM_EVENTS = "lead-item-events";
    public static final String ABANDONED_CART_EVENTS = "abandoned-cart-events";

    // TODO : Remove this topic, it is not used anymore
    // public static final String ORDER_STATUS_CHANGED = "order-status-changed";
}
