Feature: Market lifecycle classification
  Scenario Outline: classify market lifecycle from flags
    Given a market with active "<active>", closed "<closed>", enable_order_book "<enable_order_book>", accepting_orders "<accepting_orders>", and event_start_time "<event_start_time>"
    When I normalize the market
    Then the lifecycle_state is "<lifecycle_state>"

    Examples:
      | active | closed | enable_order_book | accepting_orders | event_start_time        | lifecycle_state         |
      | true   | true   | false             | false            | 2099-01-01T00:00:00Z    | CLOSED                 |
      | true   | false  | true              | true             | 2099-01-01T00:00:00Z    | OPEN_TRADABLE          |
      | true   | false  | true              | false            | 2099-01-01T00:00:00Z    | UPCOMING_NOT_TRADABLE  |
      | true   | false  | false             | false            | 1999-01-01T00:00:00Z    | OPEN_NOT_TRADABLE      |
      | true   | false  | false             | false            | 2099-01-01T00:00:00Z    | UPCOMING_NOT_TRADABLE  |
