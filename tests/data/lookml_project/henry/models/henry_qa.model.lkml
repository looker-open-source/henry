connection: "thelook"

# include all the views
include: "/views/**/*.view"


# Unused
explore: unused_explore_no_joins {
  from: view1
}

explore: unused_explore_2_joins {
  from: view1
  join: join1 {
    from: view1
    sql_on: ${unused_explore_2_joins.d1} = ${join1.d1} ;;
  }
  join: join2 {
    from: view2
    sql_on: ${unused_explore_2_joins.d1} = ${join2.d1} ;;
  }
}


# Used
explore: explore_2_joins_all_used {
  description: "This explore contains two joins, both used"
  from: view1
  join: join1 {
    from: view1
    sql_on: ${explore_2_joins_all_used.d1} = ${join1.d1} ;;
  }
  join: join2 {
    from: view2
    sql_on: ${explore_2_joins_all_used.d1} = ${join2.d1} ;;
  }
}


explore: explore_2_joins_1_used {
  from: view1
  join: join1 {
    from: view1
    sql_on: ${explore_2_joins_1_used.d1} = ${join1.d1} ;;
  }
  join: join2 {
    from: view2
    sql_on: ${explore_2_joins_1_used.d1} = ${join2.d1} ;;
  }
}
