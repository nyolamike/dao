defmodule DaoStudentGenUpdateSqlTest do
  use ExUnit.Case

  alias DaoEngine, as: Dao

  setup do
    %{
      "context" => %{
        "database_type" => "mysql",
        "database_name" => "grocerify",
        "schema" => %{
          "students" => %{
            "name" => "string"
          }
        },
        "auto_schema_changes" => [],
        "auto_alter_db" => true
      }
    }
  end

  test "generate sql to update a student where table does not exist", %{"context" => context} do
    # delete the students table from schema
    # UPDATE student SET major = 'Bio' WHERE major = 'Biology'
    context = %{context | "schema" => Map.delete(context["schema"], "students")}

    query = [
      update: [
        students_major: [
          students: %{
            "major" => "Bio",
            "dao@where" => {"major", "equals", "Biology"}
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)
    expected_results = get_expected_results()
    assert expected_results == results
  end

  defp get_expected_results() do
    %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [
          "CREATE TABLE `grocerify.students` (id INT(30) AUTO_INCREMENT NOT NULL PRIMARY KEY, major VARCHAR(30), created_at DATETIME DEFAULT CURRENT_TIMESTAMP, last_update_on DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, is_deleted TINYINT(1) NOT NULL DEFAULT 0, deleted_on DATETIME DEFAULT NULL)"
        ],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "students" => %{
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
            "major" => %{
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
        }
      },
      "root_cmd_node_list" => [
        update: [
          students_major: [
            students: %{
              "is_list" => true,
              "sql" =>
                "UPDATE `grocerify.students` SET major = 'Bio' WHERE (major = 'Biology') AND is_deleted = 0"
            }
          ]
        ]
      ]
    }
  end
end
