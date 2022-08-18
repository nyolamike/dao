defmodule GenSqlGetFixtureTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  test "generate sql for a get fixture " do
    context = %{
      database_type: "mysql",
      database_name: "grocerify",
      schema: %{},
      auto_schema_changes: [],
      auto_alter_db: true
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

    expected_results = get_expected_results();

    results_craft = Dao.gen_sql_for_get_fixture(context, query, aircraft_fixtures_kwl_node)

    expected_results_craft = get_expected_results_aircraft();

    assert expected_results == results
    assert expected_results_craft == results_craft
  end


  defp get_expected_results() do
    %{
      context: %{
        auto_alter_db: true,
        auto_schema_changes: [
          "\n          CREATE TABLE `grocerify.shops` (\n          id INT(30) PRIMARY KEY,\n          ,\n          created_at DATETIME  DEFAULT CURRENT_TIMESTAMP,\n          last_update_on DATETIME  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n          is_deleted TINYINT(1) NOT NULL DEFAULT 0\n          deleted_on DATETIME  DEFAULT NULL}\n        )\n      "
        ],
        database_name: "grocerify",
        database_type: "mysql",
        schema: %{
          shops: %{
            created_at: %{auto_increment: false, default: nil, is_primary_key: false, required: "", size: 60, sql: "DATETIME  DEFAULT CURRENT_TIMESTAMP", type: :datetime},
            deleted_on: :timestamp,
            id: :pk,
            is_deleted: :boolean,
            last_update_on: %{auto_increment: false, default: "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", is_primary_key: false, required: "", size: 60, sql: "DATETIME  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", type: :datetime},
            deleted_on_col: %{auto_increment: false, default: "NULL", is_primary_key: false, required: "", size: 60, sql: "DATETIME  DEFAULT NULL", type: :datetime},
            is_deleted_col: %{auto_increment: false, default: nil, is_primary_key: false, required: true, size: 1, sql: "TINYINT(1) NOT NULL DEFAULT 0", type: :string}
          }
        }
      },
      fixture_list: [shops: %{is_list: true, sql: "SELECT * FROM `grocerify.shops` WHERE is_deleted == 0"}]
    }
  end

  defp get_expected_results_aircraft() do
    %{
      context: %{
        auto_alter_db: true,
        auto_schema_changes: [
          "\n          CREATE TABLE `grocerify.aircraft` (\n          id INT(30) PRIMARY KEY,\n          ,\n          created_at DATETIME  DEFAULT CURRENT_TIMESTAMP,\n          last_update_on DATETIME  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n          is_deleted TINYINT(1) NOT NULL DEFAULT 0\n          deleted_on DATETIME  DEFAULT NULL}\n        )\n      "
        ],
        database_name: "grocerify",
        database_type: "mysql",
        schema: %{
          aircraft: %{
            created_at: %{auto_increment: false, default: nil, is_primary_key: false, required: "", size: 60, sql: "DATETIME  DEFAULT CURRENT_TIMESTAMP", type: :datetime},
            deleted_on: :timestamp,
            id: :pk,
            is_deleted: :boolean,
            last_update_on: %{auto_increment: false, default: "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", is_primary_key: false, required: "", size: 60, sql: "DATETIME  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", type: :datetime},
            deleted_on_col: %{auto_increment: false, default: "NULL", is_primary_key: false, required: "", size: 60, sql: "DATETIME  DEFAULT NULL", type: :datetime},
            is_deleted_col: %{auto_increment: false, default: nil, is_primary_key: false, required: true, size: 1, sql: "TINYINT(1) NOT NULL DEFAULT 0", type: :string}
          }
        }
      },
      fixture_list: [aircraft: %{is_list: false, sql: "SELECT * FROM `grocerify.aircraft` WHERE is_deleted == 0"}]
    }
  end
end
