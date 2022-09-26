defmodule DaoCmdTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  # test "can execute a shell command" do
  #   # res = System.cmd("mysql", ["-u root"])
  #   # res = System.shell("mysql -u root -p")
  #   # IO.inspect(res)
  # end

  test "using myxql" do
    # Dao.now()
    context = %{
      "database_type" => "mysql",
      "database_name" => "farmers_db",
      "schema" => %{},
      "auto_schema_changes" => [],
      "auto_alter_db" => true,
      "track_id" => "4664",
      "reset_db" => true
    }

    query = [
      get: [
        improving_where: [
          farmers: %{
            "name" => "str",
            "dao@where" => {
              {"id", ">", 5},
              "&",
              {"name", "contains", "brendah"}
            }
          }
        ]
      ]
    ]

    results = Dao.execute_real(context, query)
    assert expected_context() == results["context"]
    # assert expected_auto_schama_results(conn_id) == results["index_ref_results"]
    assert 3 == results["next_index"]
    # assert expected_root_cmd_results() == results["root_cmd_node_results_list"]
    assert expected_node_list() == results["root_cmd_node_list"]
    assert %{} == results["errors"]
    assert false == results["has_errors"]
  end

  defp expected_context() do
    %{
      "auto_alter_db" => true,
      "auto_schema_changes" => ["DROP DATABASE farmers_db",
       "CREATE DATABASE IF NOT EXISTS farmers_db",
       "CREATE TABLE `farmers` (id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, name VARCHAR(30), created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_update_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, is_deleted TINYINT(1) NOT NULL DEFAULT 0, deleted_on DATETIME DEFAULT NULL)"],
      "database_name" => "farmers_db",
      "database_type" => "mysql",
      "reset_db" => true,
      "schema" => %{
        "farmers" => %{
          "created_at" => %{
            "auto_increment" => false,
            "default" => nil,
            "is_primary_key" => false,
            "required" => "",
            "size" => 60,
            "sql" => "DATETIME DEFAULT CURRENT_TIMESTAMP",
            "type" => "datetime",
            "unique" => false
          },
          "deleted_on" => %{
            "auto_increment" => false,
            "default" => "NULL",
            "is_primary_key" => false,
            "required" => "",
            "size" => 60,
            "sql" => "DATETIME DEFAULT NULL",
            "type" => "datetime",
            "unique" => false
          },
          "id" => %{
            "auto_increment" => true,
            "default" => nil,
            "is_primary_key" => true,
            "required" => false,
            "size" => 30,
            "sql" => "INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY",
            "type" => "integer"
          },
          "is_deleted" => %{
            "auto_increment" => false,
            "default" => nil,
            "is_primary_key" => false,
            "required" => true,
            "size" => 1,
            "sql" => "TINYINT(1) NOT NULL DEFAULT 0",
            "type" => "boolean",
            "unique" => false
          },
          "last_update_on" => %{
            "auto_increment" => false,
            "default" => "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
            "is_primary_key" => false,
            "required" => "",
            "size" => 60,
            "sql" => "DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
            "type" => "datetime",
            "unique" => false
          },
          "name" => %{
            "auto_increment" => false,
            "default" => "",
            "is_primary_key" => false,
            "required" => false,
            "size" => 30,
            "sql" => "VARCHAR(30)",
            "type" => "string",
            "unique" => false
          }
        }
      },
      "track_id" => "4664"
    }
  end

  defp expected_auto_schama_results() do
    %{
      0 => %{
        "results" => {:ok,
         %MyXQL.Result{
           columns: nil,
           connection_id: 334,
           last_insert_id: 0,
           num_rows: 1,
           num_warnings: 0,
           rows: nil
         }},
        "sql" => "DROP DATABASE farmers_db"
      },
      1 => %{
        "results" => {:ok,
         %MyXQL.Result{
           columns: nil,
           connection_id: 334,
           last_insert_id: 0,
           num_rows: 1,
           num_warnings: 0,
           rows: nil
         }},
        "sql" => "CREATE DATABASE IF NOT EXISTS farmers_db"
      },
      2 => %{
        "results" => {:ok,
         %MyXQL.Result{
           columns: nil,
           connection_id: 335,
           last_insert_id: 0,
           num_rows: 0,
           num_warnings: 0,
           rows: nil
         }},
        "sql" => "CREATE TABLE `farmers` (id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, name VARCHAR(30), created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_update_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, is_deleted TINYINT(1) NOT NULL DEFAULT 0, deleted_on DATETIME DEFAULT NULL)"
      }
    }
  end

  def expected_root_cmd_results() do
    [
      get: [
        improving_where: [
          farmers: %{
            "results" => {:ok,
             %MyXQL.Result{
               columns: ["name"],
               connection_id: 335,
               last_insert_id: nil,
               num_rows: 0,
               num_warnings: 0,
               rows: []
             }},
            "sql" => "SELECT `farmers`.`name` FROM `farmers` WHERE ((`farmers`.`id` > 5) AND (`farmers`.`name` LIKE '%brendah%')) AND `farmers`.`is_deleted` = 0"
          }
        ]
      ]
    ]
  end

  def expected_node_list() do
    [
      get: [
        improving_where: [
          farmers: %{
            "is_list" => true,
            "sql" => "SELECT `farmers`.`name` FROM `farmers` WHERE ((`farmers`.`id` > 5) AND (`farmers`.`name` LIKE '%brendah%')) AND `farmers`.`is_deleted` = 0"
          }
        ]
      ]
    ]
  end
end
