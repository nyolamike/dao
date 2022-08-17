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

    expected_results = %{
      context: %{
        auto_alter_db: true,
        auto_schema_changes: [
          """
            CREATE TABLE `grocerify.shops` (
              id INT PRIMARY KEY,
              created_at
              last_update_on
              is_deleted INT
              deleted_on
            )
          """
        ],
        database_name: "grocerify",
        database_type: "mysql",
        schema: %{
          shops: %{
            created_at: :timestamp,
            deleted_on: :timestamp,
            id: :pk,
            is_deleted: :boolean,
            last_update_on: :timestamp
          }
        }
      },
      fixture_list: [
        shops: %{
          is_list: true,
          sql: "SELECT * FROM `grocerify.shops` WHERE is_deleted == 0"
        }
      ]
    }

    results_craft = Dao.gen_sql_for_get_fixture(context, query, aircraft_fixtures_kwl_node)

    expected_results_craft = %{
      context: %{
        auto_alter_db: true,
        auto_schema_changes: [
          "  CREATE TABLE `grocerify.aircraft` (\n    id INT PRIMARY KEY,\n    created_at\n    last_update_on\n    is_deleted INT\n    deleted_on\n  )\n"
        ],
        database_name: "grocerify",
        database_type: "mysql",
        schema: %{
          aircraft: %{
            created_at: :timestamp,
            deleted_on: :timestamp,
            id: :pk,
            is_deleted: :boolean,
            last_update_on: :timestamp
          }
        }
      },
      fixture_list: [
        aircraft: %{
          is_list: false,
          sql: "SELECT * FROM `grocerify.aircraft` WHERE is_deleted == 0"
        }
      ]
    }

    assert expected_results == results
    assert expected_results_craft == results_craft
  end

  test "generate sql for get root command node" do
    context = %{
      database_type: "mysql",
      database_name: "grocerify",
      schema: %{},
      auto_schema_changes: [],
      auto_alter_db: true
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

    expected_results = %{
      context: %{
        auto_alter_db: true,
        auto_schema_changes: [
          "  CREATE TABLE `grocerify.shops` (\n    id INT PRIMARY KEY,\n    created_at\n    last_update_on\n    is_deleted INT\n    deleted_on\n  )\n"
        ],
        database_name: "grocerify",
        database_type: "mysql",
        schema: %{
          shops: %{
            created_at: :timestamp,
            deleted_on: :timestamp,
            id: :pk,
            is_deleted: :boolean,
            last_update_on: :timestamp
          }
        }
      },
      input_node_list: [
        list_of_shops: [
          shops: %{is_list: true, sql: "SELECT * FROM `grocerify.shops` WHERE is_deleted == 0"}
        ]
      ]
    }

    results_aircraft = Dao.gen_sql_for_get(context, query, aircraft_get_cmd_node)

    expected_results_aircraft = %{
      context: %{
        auto_alter_db: true,
        auto_schema_changes: [
          "  CREATE TABLE `grocerify.aircraft` (\n    id INT PRIMARY KEY,\n    created_at\n    last_update_on\n    is_deleted INT\n    deleted_on\n  )\n"
        ],
        database_name: "grocerify",
        database_type: "mysql",
        schema: %{
          aircraft: %{
            created_at: :timestamp,
            deleted_on: :timestamp,
            id: :pk,
            is_deleted: :boolean,
            last_update_on: :timestamp
          }
        }
      },
      input_node_list: [
        biggest_aircraft: [
          aircraft: %{
            is_list: false,
            sql: "SELECT * FROM `grocerify.aircraft` WHERE is_deleted == 0"
          }
        ]
      ]
    }

    assert expected_results == results
    assert expected_results_aircraft == results_aircraft
  end

  test "executes get query " do
    context = %{
      database_type: "mysql",
      database_name: "grocerify",
      schema: %{},
      auto_schema_changes: [],
      auto_alter_db: true
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

    expected_results = %{
      context: %{
        auto_alter_db: true,
        auto_schema_changes: [
          "  CREATE TABLE `grocerify.aircraft` (\n    id INT PRIMARY KEY,\n    created_at\n    last_update_on\n    is_deleted INT\n    deleted_on\n  )\n",
          "  CREATE TABLE `grocerify.shops` (\n    id INT PRIMARY KEY,\n    created_at\n    last_update_on\n    is_deleted INT\n    deleted_on\n  )\n"
        ],
        database_name: "grocerify",
        database_type: "mysql",
        schema: %{
          aircraft: %{
            created_at: :timestamp,
            deleted_on: :timestamp,
            id: :pk,
            is_deleted: :boolean,
            last_update_on: :timestamp
          },
          shops: %{
            created_at: :timestamp,
            deleted_on: :timestamp,
            id: :pk,
            is_deleted: :boolean,
            last_update_on: :timestamp
          }
        }
      },
      root_cmd_node_list: [
        get: [
          biggest_aircraft: [
            aircraft: %{
              is_list: false,
              sql: "SELECT * FROM `grocerify.aircraft` WHERE is_deleted == 0"
            }
          ]
        ],
        get: [
          list_of_shops: [
            shops: %{is_list: true, sql: "SELECT * FROM `grocerify.shops` WHERE is_deleted == 0"}
          ]
        ]
      ]
    }

    assert expected_results == results
  end

  test "executes get query will not schedule tables to be auto created when auto_alter_db is set to false " do
    context = %{
      database_type: "mysql",
      database_name: "grocerify",
      schema: %{},
      auto_schema_changes: [],
      auto_alter_db: false
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

    expected_results = %{
      context: %{
        auto_alter_db: false,
        auto_schema_changes: [],
        database_name: "grocerify",
        database_type: "mysql",
        schema: %{}
      },
      root_cmd_node_list: [
        get: [
          biggest_aircraft: [
            aircraft: %{
              is_list: false,
              sql: "SELECT * FROM `grocerify.aircraft` WHERE is_deleted == 0"
            }
          ]
        ],
        get: [
          list_of_shops: [
            shops: %{is_list: true, sql: "SELECT * FROM `grocerify.shops` WHERE is_deleted == 0"}
          ]
        ]
      ]
    }

    assert expected_results == results
  end

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

    columns_sql =
      "first_name VARCHAR(50) NOT NULL, last_name VARCHAR(30) NULL, status VARCHAR(10) DEFAULT 'pending'"

    expected_results = %{
      context: %{
        auto_alter_db: true,
        auto_schema_changes: [
          "  CREATE TABLE `grocerify.employees` (\n    id INT PRIMARY KEY,\n #{columns_sql},\n    created_at\n    last_update_on\n    is_deleted INT\n    deleted_on\n  )\n"
        ],
        database_name: "grocerify",
        database_type: "mysql",
        schema: %{
          employees: %{
            created_at: :timestamp,
            deleted_on: :timestamp,
            id: :pk,
            is_deleted: :boolean,
            last_update_on: :timestamp,
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
      },
      root_cmd_node_list: [
        get: [
          employees_list: [
            employees: %{
              is_list: true,
              sql: """
                SELECT
                  `grocerify.employees.id`,
                  `grocerify.employees.first_name`,
                  `grocerify.employees.last_name`,
                  `grocerify.employees.status`
                FROM `grocerify.employees` WHERE is_deleted == 0
              """
            }
          ]
        ]
      ]
    }

    assert expected_results == results
  end
end
