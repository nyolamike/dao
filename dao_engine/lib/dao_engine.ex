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

    # nyd: There is alot of boiler plate code here, consider refactoring
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

        :update ->
          %{"context" => context, "input_node_list" => input_node_list} =
            gen_sql_for_update(results_acc["context"], query_object, input_kwl_node)

          root_cmd_node_list = [{:update, input_node_list} | results_acc["root_cmd_node_list"]]
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
      ensure_table_response =
        Table.gen_sql_ensure_table_exists(result_acc["context"], str_node_name_key, query_config)

      {context, any_alter_table_sql} =
        case ensure_table_response do
          {context, sql} -> {context, sql}
          context -> {context, ""}
        end

      insert_sql =
        if is_map(query_config) && Map.has_key?(query_config, "dao@def_only") &&
             Map.get(query_config, "dao@def_only") == true do
          # no select sql will be generated
          ""
        else
          sql = "SELECT"

          specififc_cols_sql =
            cond do
              is_list(query_config) ->
                # nyd: we expect that someone just provided a list of specific columns to be fecthed
                Enum.reduce(query_config, "", fn key, sql_acc ->
                  skip = ["dao@where", "dao@def_only"]

                  if key in skip do
                    sql_acc
                  else
                    comma = if sql_acc == "", do: "", else: ", "
                    "#{sql_acc}#{comma}#{key}"
                  end
                end)

              is_map(query_config) ->
                processed_query_config = Table.preprocess_query_config(context, query_config)

                Enum.reduce(processed_query_config["columns"], "", fn {key,
                                                                       _cast_data_type_or_formating_flags},
                                                                      sql_acc ->
                  skip = Utils.skip_keys()

                  if key in skip do
                    sql_acc
                  else
                    comma = if sql_acc == "", do: "", else: ", "
                    column_name = Column.sql_column_name(context, str_node_name_key, key)
                    "#{sql_acc}#{comma}#{column_name}"
                  end
                end)
            end

          specififc_cols_sql =
            if String.trim(specififc_cols_sql) == "", do: "*", else: specififc_cols_sql

          order_by_sql =
            if is_map(query_config) do
              processed_query_config = Table.preprocess_query_config(context, query_config)

              cond do
                Map.has_key?(processed_query_config, "order_by") &&
                    is_binary(processed_query_config["order_by"]) ->
                  # one has specified using string so we trim and split using spaces
                  orderby_list =
                    processed_query_config["order_by"]
                    |> String.trim()
                    |> String.split(trim: true)

                  Enum.reduce(orderby_list, "", fn column, sql_acc ->
                    comma = if sql_acc == "", do: "", else: ", "
                    "#{sql_acc}#{comma}#{column}"
                  end)
                  |> String.trim()

                Map.has_key?(processed_query_config, "order_by") &&
                    is_list(processed_query_config["order_by"]) ->
                  Enum.reduce(processed_query_config["order_by"], "", fn column, sql_acc ->
                    comma = if sql_acc == "", do: "", else: ", "
                    "#{sql_acc}#{comma}#{column}"
                  end)
                  |> String.trim()

                Map.has_key?(processed_query_config, "order_by_ascending") &&
                    is_binary(processed_query_config["order_by_ascending"]) ->
                  # one has specified using string so we trim and split using spaces
                  orderby_list =
                    processed_query_config["order_by_ascending"]
                    |> String.trim()
                    |> String.split(trim: true)

                  temp_sql =
                    Enum.reduce(orderby_list, "", fn column, sql_acc ->
                      comma = if sql_acc == "", do: "", else: ", "
                      "#{sql_acc}#{comma}#{column}"
                    end)

                    "#{String.trim(temp_sql)} ASC"

                Map.has_key?(processed_query_config, "order_by_ascending") &&
                    is_list(processed_query_config["order_by_ascending"]) ->
                  temp_sql =
                    Enum.reduce(processed_query_config["order_by_ascending"], "", fn column,
                                                                                     sql_acc ->
                      comma = if sql_acc == "", do: "", else: ", "
                      "#{sql_acc}#{comma}#{column}"
                    end)

                  "#{String.trim(temp_sql)} ASC"

                Map.has_key?(processed_query_config, "order_by_descending") &&
                    is_binary(processed_query_config["order_by_descending"]) ->
                  # one has specified using string so we trim and split using spaces
                  orderby_list =
                    processed_query_config["order_by_descending"]
                    |> String.trim()
                    |> String.split(trim: true)

                  temp_sql =
                    Enum.reduce(orderby_list, "", fn column, sql_acc ->
                      comma = if sql_acc == "", do: "", else: ", "
                      "#{sql_acc}#{comma}#{column}"
                    end)

                  "#{String.trim(temp_sql)} DEC"

                Map.has_key?(processed_query_config, "order_by_descending") &&
                    is_list(processed_query_config["order_by_descending"]) ->
                  temp_sql =
                    Enum.reduce(processed_query_config["order_by_descending"], "", fn column,
                                                                                      sql_acc ->
                      comma = if sql_acc == "", do: "", else: ", "
                      "#{sql_acc}#{comma}#{column}"
                    end)

                  "#{String.trim(temp_sql)} DESC"

                true ->
                  ""
              end
            else
              ""
            end

          order_by_sql = if order_by_sql == "", do: "", else: " ORDER BY #{order_by_sql}"

          "#{sql} #{specififc_cols_sql} FROM #{Table.sql_table_name(result_acc["context"], str_node_name_key)} WHERE is_deleted == 0#{order_by_sql}"
        end

      cond do
        insert_sql != "" ->
          # we have some data to update
          sql = insert_sql
          # nyd: the auto commented out alter cmd needs to be re-enable
          # as in remove 'dao@skip: ' part from "dao@skip: ALTER TABLE ... "

          fixture_config = %{
            "sql" => sql,
            "is_list" => is_list
          }

          auto_schema_changes =
            Enum.reduce(context["auto_schema_changes"], [], fn item, acc ->
              look_for = "dao@skip: " <> any_alter_table_sql
              temp_sql = if look_for == item, do: any_alter_table_sql, else: item
              acc ++ [temp_sql]
            end)

          context = %{context | "auto_schema_changes" => auto_schema_changes}
          %{"context" => context, "fixture_list" => [{node_name_key, fixture_config}]}

        insert_sql == "" && any_alter_table_sql != "" ->
          # there is no data to be updated, but we have some schema changes
          # then these are gona be fixture because they are the only queries in the root command
          fixture_config = %{
            "sql" => any_alter_table_sql,
            "is_list" => is_list
          }

          %{"context" => context, "fixture_list" => [{node_name_key, fixture_config}]}

        insert_sql == "" && any_alter_table_sql == "" ->
          fixture_config = %{
            "sql" => "",
            "is_list" => is_list
          }

          %{"context" => context, "fixture_list" => [{node_name_key, fixture_config}]}
      end
    end)
  end

  def gen_sql_for_update(context, query_object, input_kwl_node) do
    results = %{
      "context" => context,
      "input_node_list" => []
    }

    Enum.reduce(input_kwl_node, results, fn {node_name_key, fixtures_kwl_node}, results_acc ->
      %{"context" => context, "fixture_list" => fixture_list} =
        gen_sql_for_update_fixture(results_acc["context"], query_object, fixtures_kwl_node)

      input_node_list = [{node_name_key, fixture_list} | results_acc["input_node_list"]]
      %{"context" => context, "input_node_list" => input_node_list}
    end)
  end

  def gen_sql_for_update_fixture(context, _query_object, fixtures_kwl_node) do
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

      update_sql =
        if is_map(query_config) && Map.has_key?(query_config, "dao@def_only") &&
             Map.get(query_config, "dao@def_only") == true do
          # no insert sql will be generated
          ""
        else
          sql = "UPDATE #{Table.sql_table_name(context, str_node_name_key)}"

          cond do
            is_list(query_config) ->
              # nyd: if possible how would we support updating information using lists
              ""

            is_map(query_config) ->
              gen_values_sql =
                Enum.reduce(query_config, "", fn {key, value}, sql_acc ->
                  skip = ["dao@where", "dao@def_only"]

                  if key in skip do
                    sql_acc
                  else
                    comma = if sql_acc == "", do: "", else: ", "
                    str_val = Column.sql_value_format(value)
                    "#{sql_acc}#{comma}#{key} = #{str_val}"
                  end
                end)

              where_clause_config = Map.get(query_config, "dao@where")
              where_sql = process_where_clause(context, query_config, where_clause_config)
              where_sql = String.trim(where_sql)
              # this applies only if timestamps have not been turned off in the schema
              and_condition = if where_sql == "", do: "", else: "(#{where_sql}) AND "
              default_where_clause = "#{and_condition}is_deleted = 0"

              # nyd: throw an error if there are no set values
              "#{sql} SET #{gen_values_sql} WHERE #{default_where_clause}"

            true ->
              throw("Unknown query config: gen values sql in generating sqls for update fixture")
          end
        end

      cond do
        update_sql != "" ->
          # we have some data to update
          sql = update_sql

          # nyd: the auto commented out alter cmd needs to be re-enable
          # as in remove 'dao@skip: ' part from "dao@skip: ALTER TABLE ... "

          fixture_config = %{
            "sql" => sql,
            "is_list" => is_list
          }

          auto_schema_changes =
            Enum.reduce(context["auto_schema_changes"], [], fn item, acc ->
              look_for = "dao@skip: " <> any_alter_table_sql
              temp_sql = if look_for == item, do: any_alter_table_sql, else: item
              acc ++ [temp_sql]
            end)

          context = %{context | "auto_schema_changes" => auto_schema_changes}
          %{"context" => context, "fixture_list" => [{node_name_key, fixture_config}]}

        update_sql == "" && any_alter_table_sql != "" ->
          # there is no data to be updated, but we have some schema changes
          # then these are gona be fixture because they are the only queries in the root command
          fixture_config = %{
            "sql" => any_alter_table_sql,
            "is_list" => is_list
          }

          %{"context" => context, "fixture_list" => [{node_name_key, fixture_config}]}

        update_sql == "" && any_alter_table_sql == "" ->
          fixture_config = %{
            "sql" => "",
            "is_list" => is_list
          }

          %{"context" => context, "fixture_list" => [{node_name_key, fixture_config}]}
      end
    end)
  end

  def process_where_clause(_context, _query_config, nil), do: ""

  def process_where_clause(context, query_config, [left, operator, right]) do
    process_where_clause(context, query_config, {left, operator, right})
  end

  def process_where_clause(_context, _query_config, where_sql) when is_binary(where_sql),
    do: where_sql

  def process_where_clause(context, query_config, {left, operator, right}) do
    operator_sql = Operator.parse(operator)

    cond do
      is_tuple(left) == false && is_tuple(right) == false ->
        left_side = left
        possible_value = Column.sql_value_format(right)
        "#{left_side} #{operator_sql} #{possible_value}"

      is_tuple(left) == false && is_tuple(right) == true ->
        right_side = process_where_clause(context, query_config, right)
        "(#{left}) #{operator_sql} #{right_side}"

      is_tuple(left) == true && is_tuple(right) == false ->
        left_side = process_where_clause(context, query_config, right)
        possible_value = Column.sql_value_format(right)
        "#{left_side} #{operator_sql} (#{possible_value})"

      is_tuple(left) == true && is_tuple(right) == true ->
        left_side = process_where_clause(context, query_config, left)
        right_side = process_where_clause(context, query_config, right)
        "(#{left_side}) #{operator_sql} (#{right_side})"
    end
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

      cond do
        query_config == "table" || query_config == nil ->
          # deleting the entire table
          {new_context, sql} =
            Table.get_sql_remove_table(result_acc["context"], str_node_name_key)

          fixture_config = %{
            "sql" => sql,
            "is_list" => true
          }

          %{"context" => new_context, "fixture_list" => [{node_name_key, fixture_config}]}

        is_list(query_config) == false && is_tuple(query_config) == false &&
          is_map(query_config) == false &&
            (query_config == "all" || query_config == true || String.trim(query_config) == "*") ->
          # deleting data from a table
          sql = "DELETE FROM #{Table.sql_table_name(result_acc["context"], node_name_key)}"

          # this applies only if timestamps have not been turned off in the schema
          default_where_clause = " WHERE is_deleted = 0"

          fixture_config = %{
            "sql" => sql <> default_where_clause,
            "is_list" => true
          }

          %{
            "context" => result_acc["context"],
            "fixture_list" => [{node_name_key, fixture_config}]
          }

        is_map(query_config) == true || is_tuple(query_config) ->
          # deleting data from a table
          sql = "DELETE FROM #{Table.sql_table_name(result_acc["context"], node_name_key)}"

          where_config =
            cond do
              is_map(query_config) == true && Map.has_key?(query_config, "dao@where") ->
                query_config["dao@where"]

              is_map(query_config) == true && Map.has_key?(query_config, "dao@where") == false ->
                ""

              true ->
                query_config
            end

          where_sql = process_where_clause(result_acc["context"], query_config, where_config)
          where_sql = String.trim(where_sql)
          # this applies only if timestamps have not been turned off in the schema
          and_condition = if where_sql == "", do: "", else: "(#{where_sql}) AND "
          default_where_clause = "#{and_condition}is_deleted = 0"

          fixture_config = %{
            "sql" => "#{sql} WHERE #{default_where_clause}",
            "is_list" => true
          }

          %{
            "context" => result_acc["context"],
            "fixture_list" => [{node_name_key, fixture_config}]
          }

        is_list(query_config) == true ->
          # deleting some columns
          {new_context, sql} =
            Table.get_sql_remove_columns(result_acc["context"], str_node_name_key, query_config)

          # Utils.log("here", query_config)
          fixture_config = %{
            "sql" => sql,
            "is_list" => true
          }

          %{"context" => new_context, "fixture_list" => [{node_name_key, fixture_config}]}

        true ->
          throw("Unknown delete query config on #{node_name_key}")
      end

      # if query_config == "table" || query_config == true do
      #   # deleting the entire table
      #   {new_context, sql} = Table.get_sql_remove_table(result_acc["context"], str_node_name_key)

      #   fixture_config = %{
      #     "sql" => sql,
      #     "is_list" => true
      #   }

      #   %{"context" => new_context, "fixture_list" => [{node_name_key, fixture_config}]}
      # else
      #   # deleting some columns or data
      #   {new_context, sql} =
      #     Table.get_sql_remove_columns(result_acc["context"], str_node_name_key, query_config)

      #   # Utils.log("here", query_config)
      #   fixture_config = %{
      #     "sql" => sql,
      #     "is_list" => true
      #   }

      #   %{"context" => new_context, "fixture_list" => [{node_name_key, fixture_config}]}
      # end
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
