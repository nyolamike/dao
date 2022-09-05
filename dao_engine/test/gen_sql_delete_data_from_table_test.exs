defmodule GenSqlDeleteDataFromTableTest do
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

  test "generate sql to delete all data from a table", %{"context" => context} do
    query = [
      delete: [
        all_students: [
          students: "all"
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{"students" => %{"name" => "string"}}
      },
      "root_cmd_node_list" => [
        delete: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" => "DELETE FROM `grocerify.students` WHERE is_deleted = 0"
            }
          ]
        ]
      ]
    }

    assert expected == results
  end

  test "generate sql to delete all data from a table using true", %{"context" => context} do
    query = [
      delete: [
        all_students: [
          students: true
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{"students" => %{"name" => "string"}}
      },
      "root_cmd_node_list" => [
        delete: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" => "DELETE FROM `grocerify.students` WHERE is_deleted = 0"
            }
          ]
        ]
      ]
    }

    assert expected == results
  end

  test "generate sql to delete all data from a table using *", %{"context" => context} do
    query = [
      delete: [
        all_students: [
          students: "*"
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{"students" => %{"name" => "string"}}
      },
      "root_cmd_node_list" => [
        delete: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" => "DELETE FROM `grocerify.students` WHERE is_deleted = 0"
            }
          ]
        ]
      ]
    }

    assert expected == results
  end

  test "generate sql to delete all data from a table using tuples", %{"context" => context} do
    query = [
      delete: [
        all_students: [
          students: {"student_id", "e", 5}
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{"students" => %{"name" => "string"}}
      },
      "root_cmd_node_list" => [
        delete: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" =>
                "DELETE FROM `grocerify.students` WHERE (student_id = 5) AND is_deleted = 0"
            }
          ]
        ]
      ]
    }

    assert expected == results
  end

  test "generate sql to delete all data from a table using a map", %{"context" => context} do
    query = [
      delete: [
        all_students: [
          students: %{
            "dao@where" => {
              {"name", "e", "Tom"},
              "AND",
              {"major", "e", "undecided"}
            }
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{"students" => %{"name" => "string"}}
      },
      "root_cmd_node_list" => [
        delete: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" =>
                "DELETE FROM `grocerify.students` WHERE ((name = 'Tom') AND (major = 'undecided')) AND is_deleted = 0"
            }
          ]
        ]
      ]
    }

    assert expected == results
  end
end
