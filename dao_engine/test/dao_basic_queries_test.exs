defmodule DaoBasicQueriesTest do
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

  test "select all students ", %{"context" => context} do
    # query = [
    #   get: [
    #     all_students: [
    #       students: %{}
    #     ]
    #   ]
    # ]

    # results = Dao.execute(context, query)
    # expected_results = %{
    #   "context" => {%{
    #      "auto_alter_db" => true,
    #      "auto_schema_changes" => [],
    #      "database_name" => "grocerify",
    #      "database_type" => "mysql",
    #      "schema" => %{"students" => %{"name" => "string"}}
    #    }, ""},
    #   "root_cmd_node_list" => [
    #     get: [
    #       all_students: [
    #         students: %{
    #           "is_list" => true,
    #           "sql" => "SELECT * FROM `grocerify.students` WHERE is_deleted = 0"
    #         }
    #       ]
    #     ]
    #   ]
    # }

    # assert expected_results == results
  end

  test "select column that doesnot exist ", %{"context" => context} do
    query = [
      get: [
        all_students: [
          students: %{
            "name" => "string",
            "gpa" => "integer"
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_res = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => ["ALTER TABLE `grocerify.students` ADD gpa INT(30)"],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "students" => %{
            "gpa" => %{
              "auto_increment" => false,
              "default" => "",
              "is_primary_key" => false,
              "required" => false,
              "size" => 30,
              "sql" => "INT(30)",
              "type" => "integer",
              "unique" => false
            },
            "name" => "string"
          }
        }
      },
      "root_cmd_node_list" => [
        get: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" =>
                "SELECT `grocerify.students.gpa`, `grocerify.students.name` FROM `grocerify.students` WHERE is_deleted = 0"
            }
          ]
        ]
      ]
    }

    assert expected_res == results
  end

  test "select column that doesnot exist but using lists", %{"context" => context} do
    query = [
      get: [
        all_students: [
          students: ["name", "gpa"]
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_res = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => ["ALTER TABLE `grocerify.students` ADD col_2 VARCHAR(30)"],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "students" => %{
            "name" => "string",
            "col_2" => %{
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
        get: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" => "SELECT name, gpa FROM `grocerify.students` WHERE is_deleted = 0"
            }
          ]
        ]
      ]
    }

    assert expected_res == results
  end

  test "select data order by", %{"context" => context} do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "major" => "string"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      get: [
        all_students: [
          students: %{
            "name" => "string",
            "major" => "string",
            "dao@order_by" => ["name"]
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{"students" => %{"major" => "string", "name" => "string"}}
      },
      "root_cmd_node_list" => [
        get: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" =>
                "SELECT `grocerify.students.major`, `grocerify.students.name` FROM `grocerify.students` WHERE is_deleted = 0 ORDER BY name"
            }
          ]
        ]
      ]
    }

    assert expected_results == results
  end

  test "select data order by ascending", %{"context" => context} do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "major" => "string"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      get: [
        all_students: [
          students: %{
            "name" => "string",
            "major" => "string",
            "dao@order_by_ascending" => ["name"]
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{"students" => %{"major" => "string", "name" => "string"}}
      },
      "root_cmd_node_list" => [
        get: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" =>
                "SELECT `grocerify.students.major`, `grocerify.students.name` FROM `grocerify.students` WHERE is_deleted = 0 ORDER BY name ASC"
            }
          ]
        ]
      ]
    }

    assert expected_results == results
  end

  test "select data order by descending", %{"context" => context} do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "major" => "string"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      get: [
        all_students: [
          students: %{
            "name" => "string",
            "major" => "string",
            "dao@order_by_descending" => ["name"]
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{"students" => %{"major" => "string", "name" => "string"}}
      },
      "root_cmd_node_list" => [
        get: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" =>
                "SELECT `grocerify.students.major`, `grocerify.students.name` FROM `grocerify.students` WHERE is_deleted = 0 ORDER BY name DESC"
            }
          ]
        ]
      ]
    }

    assert expected_results == results
  end

  test "select data order by with no columns specified", %{"context" => context} do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "major" => "string"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      get: [
        all_students: [
          students: %{
            "dao@order_by" => ["name"]
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{"students" => %{"major" => "string", "name" => "string"}}
      },
      "root_cmd_node_list" => [
        get: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" => "SELECT * FROM `grocerify.students` WHERE is_deleted = 0 ORDER BY name"
            }
          ]
        ]
      ]
    }

    assert expected_results == results
  end

  test "select data order by multiple columns", %{"context" => context} do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "major" => "string"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      get: [
        all_students: [
          students: %{
            "dao@order_by" => ["major", "name"]
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{"students" => %{"major" => "string", "name" => "string"}}
      },
      "root_cmd_node_list" => [
        get: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" =>
                "SELECT * FROM `grocerify.students` WHERE is_deleted = 0 ORDER BY major, name"
            }
          ]
        ]
      ]
    }

    assert expected_results == results
  end

  test "select data limit number of results", %{"context" => context} do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "major" => "string"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      get: [
        all_students: [
          students: %{
            "dao@limit" => 4
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{"students" => %{"major" => "string", "name" => "string"}}
      },
      "root_cmd_node_list" => [
        get: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" => "SELECT * FROM `grocerify.students` WHERE is_deleted = 0 LIMIT 4"
            }
          ]
        ]
      ]
    }

    assert expected_results == results
  end

  test "select data limit number of results and order by in the same query", %{
    "context" => context
  } do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "major" => "string",
          "student_id" => "pk"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      get: [
        all_students: [
          students: %{
            "dao@descend" => ["student_id"],
            "dao@limit" => 4
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "students" => %{"major" => "string", "name" => "string", "student_id" => "pk"}
        }
      },
      "root_cmd_node_list" => [
        get: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" =>
                "SELECT * FROM `grocerify.students` WHERE is_deleted = 0 ORDER BY student_id DESC LIMIT 4"
            }
          ]
        ]
      ]
    }

    assert expected_results == results
  end

  test "select data less than and not equals", %{"context" => context} do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "major" => "string",
          "student_id" => "pk"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      get: [
        all_students: [
          students: %{
            "dao@where" => {
              {"student_id", "<=", 3},
              "AND",
              {"name", "!=", "Jack"}
            }
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "students" => %{"major" => "string", "name" => "string", "student_id" => "pk"}
        }
      },
      "root_cmd_node_list" => [
        get: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" =>
                "SELECT * FROM `grocerify.students` WHERE ((student_id <= 3) AND (name <> 'Jack')) AND is_deleted = 0"
            }
          ]
        ]
      ]
    }

    assert expected_results == results
  end

  test "select data in operator", %{"context" => context} do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "major" => "string",
          "student_id" => "pk"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      get: [
        all_students: [
          students: %{
            "dao@where" => {"name", "in", ["Clare", "Kate", "Mike"]}
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "students" => %{"major" => "string", "name" => "string", "student_id" => "pk"}
        }
      },
      "root_cmd_node_list" => [
        get: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" =>
                "SELECT * FROM `grocerify.students` WHERE (name IN ('Clare', 'Kate', 'Mike')) AND is_deleted = 0"
            }
          ]
        ]
      ]
    }

    # nyd: imagine in this type of thing that the list ["Clare", "Kate", "Mike"]
    # nyd: could have been first queried and then used here

    assert expected_results == results
  end

  test "select data in operator and greater than", %{"context" => context} do
    context = %{
      "database_type" => "mysql",
      "database_name" => "grocerify",
      "schema" => %{
        "students" => %{
          "name" => "string",
          "major" => "string",
          "student_id" => "pk"
        }
      },
      "auto_schema_changes" => [],
      "auto_alter_db" => true
    }

    query = [
      get: [
        all_students: [
          students: %{
            "dao@where" => {
              {"name", "in", ["Clare", "Kate", "Mike"]},
              "AND",
              {"student_id", ">", 2}
            }
          }
        ]
      ]
    ]

    results = Dao.execute(context, query)

    expected_results = %{
      "context" => %{
        "auto_alter_db" => true,
        "auto_schema_changes" => [],
        "database_name" => "grocerify",
        "database_type" => "mysql",
        "schema" => %{
          "students" => %{"major" => "string", "name" => "string", "student_id" => "pk"}
        }
      },
      "root_cmd_node_list" => [
        get: [
          all_students: [
            students: %{
              "is_list" => true,
              "sql" =>
                "SELECT * FROM `grocerify.students` WHERE ((name IN ('Clare', 'Kate', 'Mike')) AND (student_id > 2)) AND is_deleted = 0"
            }
          ]
        ]
      ]
    }

    # nyd: imagine in this type of thing that the list ["Clare", "Kate", "Mike"]
    # nyd: could have been first queried and then used here

    assert expected_results == results
  end
end
