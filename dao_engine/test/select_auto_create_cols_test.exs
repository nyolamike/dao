defmodule SelectAutoCreateColsTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  test "auto create columns during selection   " do
    context = %{
      database_type: "mysql",
      database_name: "grocerify",
      schema: %{},
      auto_schema_changes: [],
      auto_alter_db: true
    }

    query = [
      get: [
        employees_list: [
          employees: %{
            use_default_pk: false,
            columns: %{
              emp_id: :pk,
              first_name: %{
                type: :string,
                size: 50,
                required: true
              },
              last_name: :string,
              status: %{
                type: :string,
                size: 10,
                default: "pending"
              }
            }
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    expected_results = get_expected_results();

    assert expected_results == results
  end

  defp get_expected_results() do
    %{
      context: %{
        auto_alter_db: true,
        auto_schema_changes: [
          "\n          CREATE TABLE `grocerify.employees` (\n          \n          emp_id INT(30) PRIMARY KEY,\n first_name VARCHAR (50) NOT NULL ,\n last_name VARCHAR (30)  ,\n status VARCHAR (10)  DEFAULT 'pending',\n          created_at DATETIME  DEFAULT CURRENT_TIMESTAMP,\n          last_update_on DATETIME  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n          is_deleted TINYINT(1) NOT NULL DEFAULT 0\n          deleted_on DATETIME  DEFAULT NULL}\n        )\n      "
        ],
        database_name: "grocerify",
        database_type: "mysql",
        schema: %{
          employees: %{
            created_at: %{auto_increment: false, default: nil, is_primary_key: false, required: "", size: 60, sql: "DATETIME  DEFAULT CURRENT_TIMESTAMP", type: :datetime},
            first_name: %{
              required: true,
              size: 50,
              type: :string,
              auto_increment: false,
              default: "",
              is_primary_key: false,
              sql: "VARCHAR (50) NOT NULL "
            },
            last_name: %{auto_increment: false, default: "", is_primary_key: false, required: false, size: 30, sql: "VARCHAR (30)  ", type: :string},
            last_update_on: %{auto_increment: false, default: "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", is_primary_key: false, required: "", size: 60, sql: "DATETIME  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", type: :datetime},
            status: %{
              default: "pending",
              size: 10,
              type: :string,
              auto_increment: false,
              is_primary_key: false,
              required: false,
              sql: "VARCHAR (10)  DEFAULT 'pending'"
            },
            deleted_on_col: %{auto_increment: false, default: "NULL", is_primary_key: false, required: "", size: 60, sql: "DATETIME  DEFAULT NULL", type: :datetime},
            emp_id: %{auto_increment: true, default: nil, is_primary_key: true, required: false, size: 30, sql: "INT(30) PRIMARY KEY", type: :integer},
            is_deleted_col: %{auto_increment: false, default: nil, is_primary_key: false, required: true, size: 1, sql: "TINYINT(1) NOT NULL DEFAULT 0", type: :string}
          }
        }
      },
      root_cmd_node_list: [
        get: [employees_list: [employees: %{is_list: true, sql: "SELECT * FROM `grocerify.employees` WHERE is_deleted == 0"}]]
      ]
    }
  end

end
