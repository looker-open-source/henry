view: view1 {
  sql_table_name: demo_db2.orders ;;

  dimension: d1 {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: d2 {
    type: string
    sql:  ${TABLE}.traffic_source ;;
  }

  dimension: d3 {
    type: string
    sql: ${TABLE}.status ;;
  }
  measure: m1 {
    type: count
  }
}


view: view2 {
  extends: [view1]

  dimension: d4 {
    description: "only quried in filters"
    sql: ${TABLE}.status ;;
  }
}

view: dimensions_only {
  dimension: d1 {}
  dimension: d2 {}
}
