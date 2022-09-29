defmodule DaoMysqlFarmersAppTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  test "develope a farmers application" do
    context = initial_context()

    # register farms
    # register farmers
    # a farmer is one who works on a farmer
    # a farmer can be deployed to work on multiple farms
    # record each day that a famer works on a certain farm

    # changing requirement/ update
    # modify the db so that we can record if a farmer owns a certain farm

    # register farms
    query = [
      add: [
        register_farms: [
          farms: [
            %{
              "name" => "bananas farm",
              "location" => "kampala",
              "width" => 50,
              "height" => 100,
              "size" => "50 x 100"
            },
            %{
              "name" => "general animal farm",
              "location" => "jinja",
              "width" => 400,
              "height" => 400
            }
          ]
        ]
      ]
    ]

    results = Dao.execute(context, query)
    IO.inspect(results)
  end

  defp initial_context() do
    %{
      "database_type" => "mysql",
      "database_name" => "agric",
      "schema" => %{},
      "auto_schema_changes" => [],
      "auto_alter_db" => true,
      "track_id" => "farmers_app",
      "reset_db" => true,
      "remove_sql_results_connection_ids" => true
    }
  end

  def res() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "CREATE TABLE `farms` (id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, height INT(30), location VARCHAR(30), name VARCHAR(30), size VARCHAR(30), width INT(30), created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_update_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, is_deleted TINYINT(1) NOT NULL DEFAULT 0, deleted_on DATETIME DEFAULT NULL)"
        ],
        "database_name" => "farmers_app",
        "database_type" => "mysql",
        "remove_sql_results_connection_ids" => true,
        "reset_db" => true,
        "schema" => %{
          "farms" => %{
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
            "height" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
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
            "location" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
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
            },
            "size" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "VARCHAR(30)",
              "type" => "string",
              "unique" => false
            },
            "width" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "unique" => false
            }
          }
        },
        "track_id" => "farmers_app"
      },
      "root_cmd_node_list" => [
        add: [
          register_farms: [
            farms: %{
              "is_list" => true,
              "sql" =>
                "INSERT INTO `farms`(height, location, name, width) VALUES(400, 'jinja', 'general animal farm', 400); INSERT INTO `farms`(height, location, name, size, width) VALUES(100, 'kampala', 'bananas farm', '50 x 100', 50)"
            }
          ]
        ]
      ]
    }
  end
end
