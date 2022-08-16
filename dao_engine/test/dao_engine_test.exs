defmodule DaoEngineTest do
  use ExUnit.Case
  doctest DaoEngine

  alias DaoEngine, as: Dao

  test "indicates a word is plural or false" do
    query_config = %{}
    assert true == Dao.is_word_plural?(query_config, "shops")
    assert false == Dao.is_word_plural?(query_config, "shop")
    # by default
    assert true == Dao.is_word_plural?(query_config, "aircraft")
    assert true == Dao.is_word_plural?(query_config, "deer")
    # forcing singluar on umbigious situations
    assert false == Dao.is_word_plural?(%{is_list: false}, "aircraft")
    assert false == Dao.is_word_plural?(%{is_list: false}, "deer")
  end

  test "handling unknown root command " do
    shop_fixtures_kwl_node = [
      # a fixture
      shops: %{}
    ]

    query_object = [
      get: [
        list_of_shops: shop_fixtures_kwl_node
      ],
      unknown_cmd: false
    ]
  end

  test "generate sql for a get fixture " do
    context = %{
      database_name: "grocerify"
    }

    shop_fixtures_kwl_node = [
      # a fixture
      shops: %{}
    ]

    aircraft_fixtures_kwl_node = [
      aircraft: %{
        is_list: false
      }
    ]

    query = [
      get: [
        list_of_shops: shop_fixtures_kwl_node
      ],
      get: [
        biggest_aircraft: aircraft_fixtures_kwl_node
      ]
    ]

    results = Dao.gen_sql_for_get_fixture(context, query, shop_fixtures_kwl_node)

    expected_results = [
      shops: %{
        sql: "SELECT * FROM `#{context.database_name}.shops` WHERE is_deleted == 0",
        is_list: true
      }
    ]

    results_craft = Dao.gen_sql_for_get_fixture(context, query, aircraft_fixtures_kwl_node)

    expected_results_craft = [
      aircraft: %{
        sql: "SELECT * FROM `#{context.database_name}.aircraft` WHERE is_deleted == 0",
        is_list: false
      }
    ]

    assert expected_results == results
    assert expected_results_craft == results_craft
  end

  test "generate sql for get root command node" do
    context = %{
      database_name: "grocerify"
    }

    shop_get_cmd_node = [
      list_of_shops: [
        # a fixture
        shops: %{}
      ]
    ]

    aircraft_get_cmd_node = [
      biggest_aircraft: [
        aircraft: %{
          is_list: false
        }
      ]
    ]

    query = [
      get: shop_get_cmd_node,
      get: aircraft_get_cmd_node
    ]

    results = Dao.gen_sql_for_get(context, query, shop_get_cmd_node)

    expected_results = [
      list_of_shops: [
        shops: %{
          sql: "SELECT * FROM `#{context.database_name}.shops` WHERE is_deleted == 0",
          is_list: true
        }
      ]
    ]

    results_aircraft = Dao.gen_sql_for_get(context, query, aircraft_get_cmd_node)

    expected_results_aircraft = [
      biggest_aircraft: [
        aircraft: %{
          sql: "SELECT * FROM `#{context.database_name}.aircraft` WHERE is_deleted == 0",
          is_list: false
        }
      ]
    ]

    assert expected_results == results
    assert expected_results_aircraft == results_aircraft
  end

  test "executes get query " do
    context = %{
      database_name: "grocerify"
    }

    shop_get_cmd_node = [
      list_of_shops: [
        # a fixture
        shops: %{}
      ]
    ]

    aircraft_get_cmd_node = [
      biggest_aircraft: [
        aircraft: %{
          is_list: false
        }
      ]
    ]

    query = [
      get: shop_get_cmd_node,
      get: aircraft_get_cmd_node
    ]

    results = Dao.execute(context, query)

    expected_results = [
      [
        list_of_shops: [
          shops: %{
            is_list: true,
            sql: "SELECT * FROM `#{context.database_name}.shops` WHERE is_deleted == 0"
          }
        ]
      ],
      [
        biggest_aircraft: [
          aircraft: %{
            is_list: false,
            sql: "SELECT * FROM `#{context.database_name}.aircraft` WHERE is_deleted == 0"
          }
        ]
      ]
    ]

    assert expected_results == results
  end
end
