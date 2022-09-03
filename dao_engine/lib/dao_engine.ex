defmodule DaoEngine do
  @moduledoc """
  Documentation for `DaoEngine`.
  """

  import Inflex

  import CodeStats.ConfigHelpers

  alias DaoEngine, as: Dao

  @default_table_schema %{
    "id" => "pk",
    "created_at" => "timestamp",
    "last_update_on" => "timestamp",
    "is_deleted" => "boolean",
    "deleted_on" => "timestamp"
  }

  @doc """
  Hello world.

  ## Examples

      iex> DaoEngine.hello()
      :world

  """
  def hello do
    :world
  end

  def execute(context, query_object) do
    # query object have the following root keys
    # get
    # add
    # edit
    # delete

    # nyd: reverse auto_schema_changes list before executing its queries
    results = %{
      "context" => context,
      "root_cmd_node_list" => []
    }

    Enum.reduce(query_object, results, fn {command_key, input_kwl_node}, results_acc ->
      case command_key do
        :add ->
          %{"context" => context, "input_node_list" => input_node_list} =
            gen_sql_for_add(results_acc["context"], query_object, input_kwl_node)

          root_cmd_node_list = [{:add, input_node_list} | results_acc["root_cmd_node_list"]]
          %{"context" => context, "root_cmd_node_list" => root_cmd_node_list}

        :get ->
          %{"context" => context, "input_node_list" => input_node_list} =
            gen_sql_for_get(results_acc["context"], query_object, input_kwl_node)

          root_cmd_node_list = [{:get, input_node_list} | results_acc["root_cmd_node_list"]]
          %{"context" => context, "root_cmd_node_list" => root_cmd_node_list}

        :delete ->
          %{"context" => context, "input_node_list" => input_node_list} =
            gen_sql_for_delete(results_acc["context"], query_object, input_kwl_node)

          root_cmd_node_list = [{:delete, input_node_list} | results_acc["root_cmd_node_list"]]
          %{"context" => context, "root_cmd_node_list" => root_cmd_node_list}

        _ ->
          "UNKNOWN"
      end
    end)
  end

  @spec gen_sql_for_add(map(), keyword(), keyword()) :: map()
  def gen_sql_for_add(context, query_object, input_kwl_node) do
    results = %{
      "context" => context,
      "input_node_list" => []
    }

    Enum.reduce(input_kwl_node, results, fn {node_name_key, fixtures_kwl_node}, results_acc ->
      %{"context" => context, "fixture_list" => fixture_list} =
        gen_sql_for_add_fixture(results_acc["context"], query_object, fixtures_kwl_node)

      input_node_list = [{node_name_key, fixture_list} | results_acc["input_node_list"]]
      %{"context" => context, "input_node_list" => input_node_list}
    end)
  end

  @spec gen_sql_for_add_fixture(map(), keyword(), keyword()) :: map()
  def gen_sql_for_add_fixture(context, _query_object, fixtures_kwl_node) do
    result = %{
      "context" => context,
      "fixture_list" => []
    }

    Enum.reduce(fixtures_kwl_node, result, fn {node_name_key, query_config}, result_acc ->
      str_node_name_key = Atom.to_string(node_name_key)
      is_list = Utils.is_word_plural?(query_config, str_node_name_key)
      # check if the table and its cols exist in the schema
      ensure_table_response =
        Table.gen_sql_ensure_table_exists(result_acc["context"], str_node_name_key, query_config)

      # Utils.log("ensure_table_exist", ensure_table_response)

      {context, any_alter_table_sql} =
        case ensure_table_response do
          {context, sql} -> {context, sql}
          context -> {context, ""}
        end

      insert_cols_sql =
        if is_map(query_config) && Map.has_key?(query_config, "dao@def_only") &&
             Map.get(query_config, "dao@def_only") == true do
          # no insert sql will be generated
          ""
        else
          sql = "INSERT INTO #{Table.sql_table_name(context, str_node_name_key)}"

          cond do
            is_list(query_config) ->
              [h | _t] = query_config

              # remeber that if we have new auto inserted cols like col_x, col_y these can cause the query to be wrong
              # so we need a way to keep our data organised accordingly
              gen_values_sql =
                if is_list(h) do
                  # we are dealing with an array of arrays
                  Enum.reduce(h, "", fn array_of_values, sql_acc ->
                    array_values_sql =
                      Enum.reduce(array_of_values, "", fn value, sql_acc ->
                        str_val = Column.sql_value_format(value)
                        comma = if sql_acc == "", do: "", else: ", "
                        sql_acc <> comma <> str_val
                      end)

                    "(#{array_values_sql})"
                    sql_acc <> array_values_sql
                  end)
                else
                  # the query_config is single array of values
                  values_sql =
                    Enum.reduce(query_config, "", fn value, sql_acc ->
                      str_val = Column.sql_value_format(value)
                      comma = if sql_acc == "", do: "", else: ", "
                      sql_acc <> comma <> str_val
                    end)

                  "(#{values_sql})"
                end

              "#{sql} VALUES#{gen_values_sql}"

            is_map(query_config) ->
              acc_4_map = %{
                "col_names_sql" => "",
                "col_vals_sql" => ""
              }

              gen_values_sql =
                Enum.reduce(query_config, acc_4_map, fn {key, value}, acc ->
                  skip = ["dao@def_only"]

                  if key in skip do
                    acc
                  else
                    col_comma = if acc["col_names_sql"] == "", do: "", else: ", "
                    cols_sql_acc = acc["col_names_sql"] <> col_comma <> key

                    str_val = Column.sql_value_format(value)
                    comma = if acc["col_vals_sql"] == "", do: "", else: ", "
                    sql_acc = acc["col_vals_sql"] <> comma <> str_val
                    %{acc | "col_names_sql" => cols_sql_acc, "col_vals_sql" => sql_acc}
                  end
                end)

              # nyd: throw an error if there are no columns or values
              "#{sql}(#{gen_values_sql["col_names_sql"]}) VALUES(#{gen_values_sql["col_vals_sql"]})"

            true ->
              throw("Unknown query config: gen values sql")
          end
        end

      # Utils.log("insert_cols_sql", insert_cols_sql, is_list(query_config))

      cond do
        insert_cols_sql != "" ->
          # we have some data to insert
          sql = insert_cols_sql

          # nyd: the auto commented out alter cmd needs to be renable
          # as in remove 'dao@skip: ' part from "dao@skip: ALTER TABLE ... "

          fixture_config = %{
            "sql" => sql,
            "is_list" => is_list
          }

          # Utils.log("any_alter_table_sql", any_alter_table_sql, is_list(query_config))

          auto_schema_changes =
            Enum.reduce(context["auto_schema_changes"], [], fn item, acc ->
              look_for = "dao@skip: " <> any_alter_table_sql
              temp_sql = if look_for == item, do: any_alter_table_sql, else: item
              acc ++ [temp_sql]
            end)

          context = %{context | "auto_schema_changes" => auto_schema_changes}
          %{"context" => context, "fixture_list" => [{node_name_key, fixture_config}]}

        insert_cols_sql == "" && any_alter_table_sql != "" ->
          # there is no data to be inserted, but we have some schema changes
          # then these are gona be fixture because they are the only queries in the root command
          fixture_config = %{
            "sql" => any_alter_table_sql,
            "is_list" => is_list
          }

          %{"context" => context, "fixture_list" => [{node_name_key, fixture_config}]}

        insert_cols_sql == "" && any_alter_table_sql == "" ->
          fixture_config = %{
            "sql" => "",
            "is_list" => is_list
          }

          %{"context" => context, "fixture_list" => [{node_name_key, fixture_config}]}
      end
    end)
  end

  @spec gen_sql_for_get(map(), keyword(), keyword()) :: map()
  def gen_sql_for_get(context, query_object, input_kwl_node) do
    results = %{
      "context" => context,
      "input_node_list" => []
    }

    Enum.reduce(input_kwl_node, results, fn {node_name_key, fixtures_kwl_node}, results_acc ->
      %{"context" => context, "fixture_list" => fixture_list} =
        gen_sql_for_get_fixture(results_acc["context"], query_object, fixtures_kwl_node)

      input_node_list = [{node_name_key, fixture_list} | results_acc["input_node_list"]]
      %{"context" => context, "input_node_list" => input_node_list}
    end)
  end

  @spec gen_sql_for_get_fixture(map(), keyword(), keyword()) :: map()
  def gen_sql_for_get_fixture(context, _query_object, fixtures_kwl_node) do
    result = %{
      "context" => context,
      "fixture_list" => []
    }

    Enum.reduce(fixtures_kwl_node, result, fn {node_name_key, query_config}, result_acc ->
      str_node_name_key = Atom.to_string(node_name_key)
      is_list = Utils.is_word_plural?(query_config, str_node_name_key)
      # check if the table exists in the schema
      context =
        Table.gen_sql_ensure_table_exists(result_acc["context"], str_node_name_key, query_config)

      sql = "SELECT *"
      sql = sql <> " FROM #{Table.sql_table_name(result_acc["context"], str_node_name_key)}"
      sql = sql <> " WHERE is_deleted == 0"

      fixture_config = %{
        "sql" => sql,
        "is_list" => is_list
      }

      %{"context" => context, "fixture_list" => [{node_name_key, fixture_config}]}
    end)
  end

  def gen_sql_for_delete(context, query_object, input_kwl_node) do
    results = %{
      "context" => context,
      "input_node_list" => []
    }

    Enum.reduce(input_kwl_node, results, fn {node_name_key, fixtures_kwl_node}, results_acc ->
      %{"context" => context, "fixture_list" => fixture_list} =
        gen_sql_for_delete_fixture(results_acc["context"], query_object, fixtures_kwl_node)

      input_node_list = [{node_name_key, fixture_list} | results_acc["input_node_list"]]
      %{"context" => context, "input_node_list" => input_node_list}
    end)
  end

  def gen_sql_for_delete_fixture(context, _query_object, fixtures_kwl_node) do
    result = %{
      "context" => context,
      "fixture_list" => []
    }

    Enum.reduce(fixtures_kwl_node, result, fn {node_name_key, query_config}, result_acc ->
      str_node_name_key = Atom.to_string(node_name_key)

      if query_config == "table" || query_config == true do
        # deleting the entire table
        {new_context, sql} = Table.get_sql_remove_table(result_acc["context"], str_node_name_key)

        fixture_config = %{
          "sql" => sql,
          "is_list" => true
        }

        %{"context" => new_context, "fixture_list" => [{node_name_key, fixture_config}]}
      else
        # deleting some columns or data
        {new_context, sql} =
          Table.get_sql_remove_columns(result_acc["context"], str_node_name_key, query_config)

        # Utils.log("here", query_config)
        fixture_config = %{
          "sql" => sql,
          "is_list" => true
        }

        %{"context" => new_context, "fixture_list" => [{node_name_key, fixture_config}]}
      end
    end)
  end

  def load_config_from_file(project_path_slug) do
    base_path = get_env("BASE_ROOT_FILE_PATH")
    path = Path.join([base_path, project_path_slug, "dao.json"])
    path = Path.expand(path)

    with {:ok, body} <- File.read(path),
         {:ok, project_config} <- Jason.decode(body) do
      # add expected runtime keys
      project_config =
        project_config
        |> Utils.ensure_key("auto_schema_changes", [])
        |> Utils.ensure_key("auto_allowed_names_queries_changes", [])
        |> Utils.ensure_key("migrations", [])
        |> Utils.ensure_key("seeds", [])

      {:ok, project_config}
    else
      {:error, reason} ->
        {:error, "Failed to read database configuration file for project:#{project_path_slug}",
         reason}
    end
  end
end
