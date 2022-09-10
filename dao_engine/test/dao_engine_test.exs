defmodule DaoEngineTest do
  use ExUnit.Case
  doctest DaoEngine

  alias DaoEngine, as: Dao

  setup do
    %{
      "context" => %{
        "database_type" => "mysql",
        "database_name" => "grocerify",
        "schema" => %{},
        "auto_schema_changes" => [],
        "auto_alter_db" => false
      }
    }
  end

  test "indicates a word is plural or false" do
    query_config = %{}
    assert true == Utils.is_word_plural?(query_config, "shops")
    assert false == Utils.is_word_plural?(query_config, "shop")
    # by default
    assert true == Utils.is_word_plural?(query_config, "aircraft")
    assert true == Utils.is_word_plural?(query_config, "deer")
    # forcing singluar on umbigious situations
    assert false == Utils.is_word_plural?(%{"is_list" => false}, "aircraft")
    assert false == Utils.is_word_plural?(%{"is_list" => false}, "deer")
  end

  test "handling unknown root command " do
    # nyd: define the body of this test
    # shop_fixtures_kwl_node = [
    #   # a fixture
    #   shops: %{}
    # ]

    # query_object = [
    #   get: [
    #     list_of_shops: shop_fixtures_kwl_node
    #   ],
    #   unknown_cmd: false
    # ]
  end

  test "executes get query will not schedule tables to be auto created when auto_alter_db is set to false ",
       %{"context" => context} do
    shop_get_cmd_node = [
      list_of_shops: [
        # a fixture
        shops: %{}
      ]
    ]

    aircraft_get_cmd_node = [
      biggest_aircraft: [
        aircraft: %{
          "is_list" => false
        }
      ]
    ]

    query = [
      get: shop_get_cmd_node,
      get: aircraft_get_cmd_node
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => false,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{}
      },
      "root_cmd_node_list" => [
        get: [
          biggest_aircraft: [
            aircraft: %{
              "is_list" => false,
              "sql" => "SELECT * FROM `grocerify.aircraft` WHERE is_deleted = 0"
            }
          ]
        ],
        get: [
          list_of_shops: [
            shops: %{
              "is_list" => true,
              "sql" => "SELECT * FROM `grocerify.shops` WHERE is_deleted = 0"
            }
          ]
        ]
      ]
    }

    assert expected_results == results
  end
end
