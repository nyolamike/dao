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

      # Utils.log("flanken", query_config, str_node_name_key == "employeexes")

      # Utils.log("ensure_table_exist", ensure_table_response)

      {context, any_alter_table_sql, is_def_only} =
        case ensure_table_response do
          {context, sql} -> {context, sql, false}
          {context, sql, is_def_only} -> {context, sql, is_def_only}
          context -> {context, "", false}
        end

      insert_cols_sql =
        if (is_map(query_config) && Map.has_key?(query_config, "dao@def_only") &&
              Map.get(query_config, "dao@def_only") == true) || is_def_only == true do
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
                  Enum.reduce(query_config, "", fn array_of_values, sql_acc ->
                    array_values_sql =
                      Enum.reduce(array_of_values, "", fn value, sql_acc ->
                        str_val = Column.sql_value_format(value)
                        comma = if sql_acc == "", do: "", else: ", "
                        sql_acc <> comma <> str_val
                      end)

                    comma = if sql_acc == "", do: "", else: ", "
                    "#{sql_acc}#{comma}(#{array_values_sql})"
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
                  skip = Utils.skip_keys()

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
              cols = String.trim(gen_values_sql["col_names_sql"])
              values = String.trim(gen_values_sql["col_vals_sql"])
              if values == "", do: "", else: "#{sql}(#{cols}) VALUES(#{values})"

            true ->
              throw("Unknown query config: gen values sql")
          end
        end

      # lets try to work on the foreign keys if any

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
      if node_name_key == :dao@combine do
        init_comb_acc = %{
          "context" => result_acc["context"],
          "fixture_list" => []
        }

        unions_temp =
          Enum.reduce(query_config, init_comb_acc, fn {comb_node_name_key, comb_query_config},
                                                      comb_acc ->
            temp_res =
              gen_sql_for_get_fixture_helper(result_acc, comb_node_name_key, comb_query_config)

            sql_map = Keyword.get(temp_res["fixture_list"], comb_node_name_key)
            sql_to_combine = sql_map["sql"]

            %{
              "context" => temp_res["context"],
              "fixture_list" => comb_acc["fixture_list"] ++ [sql_to_combine]
            }
          end)

        union_sql = Enum.join(unions_temp["fixture_list"], " UNION ")

        fixture_config = %{
          "sql" => union_sql,
          "is_list" => true
        }

        %{
          "context" => unions_temp["context"],
          "fixture_list" => [{node_name_key, fixture_config} | result_acc["fixture_list"]]
        }
      else
        gen_sql_for_get_fixture_helper(result_acc, node_name_key, query_config)
      end
    end)
  end

  def gen_sql_for_get_fixture_helper(
        result_acc,
        node_name_key,
        query_config,
        return_parts \\ false
      ) do
    str_node_name_key = Atom.to_string(node_name_key)
    is_list = Utils.is_word_plural?(query_config, str_node_name_key)
    # check if the table exists in the schema
    ensure_table_response =
      Table.gen_sql_ensure_table_exists(result_acc["context"], str_node_name_key, query_config)

    # {context, any_alter_table_sql} =
    #   case ensure_table_response do
    #     {context, sql} -> {context, sql}
    #     context -> {context, ""}
    #   end

    {context, any_alter_table_sql, is_def_only} =
      case ensure_table_response do
        {context, sql} -> {context, sql, true}
        {context, sql, is_def_only} -> {context, sql, is_def_only}
        context -> {context, "", true}
      end

    insert_sql =
      if is_map(query_config) && Map.has_key?(query_config, "dao@def_only") &&
           Map.get(query_config, "dao@def_only") == true do
        # no select sql will be generated
        ""
      else
        sql = "SELECT"
        %{"sql_acc" => specififc_cols_sql, "joins" => joins_list } =
          cond do
            is_list(query_config) ->
              # nyd: we expect that someone just provided a list of specific columns to be fecthed
              temp_res =
              Enum.reduce(query_config, "", fn key, sql_acc ->
                skip = Utils.skip_keys()

                if key in skip do
                  sql_acc
                else
                  comma = if sql_acc == "", do: "", else: ", "
                  column_name = Column.sql_column_name(context, str_node_name_key, key)
                  "#{sql_acc}#{comma}#{column_name}"
                end
              end)
              %{"sql_acc" => temp_res, "joins" => [] }

            is_map(query_config) ->
              processed_query_config = Table.preprocess_query_config(context, query_config)

              cols_res_acc = %{
                "sql_acc" => "",
                "joins" => []
              }

              Enum.reduce(processed_query_config["columns"], cols_res_acc, fn {key, format_config}, %{ "sql_acc" => sql_acc, "joins" => joins_acc} ->
                skip = Utils.skip_keys()
                col_flags = Utils.col_flags()

                cond do
                  key in skip && key in col_flags == false ->
                    %{"sql_acc" => sql_acc, "joins" => joins_acc }

                  key in skip && key in col_flags == true ->
                    # processable column flag
                    cond do
                      key in ["dao@unique", "dao@distinct"] ->
                        distinct_sql =
                          Enum.reduce(format_config, "", fn column_name, acc_uniq ->
                            comma = if acc_uniq == "", do: "", else: ", "

                            column_name =
                              Column.sql_column_name(context, str_node_name_key, column_name)

                            "#{acc_uniq}#{comma}#{column_name}"
                          end)

                        temp_sql = "DISTINCT #{distinct_sql}"
                        %{"sql_acc" => temp_sql, "joins" => joins_acc }
                      key in ["dao@count"] ->
                        distinct_sql =
                          Enum.reduce(format_config, "", fn column_name, acc_uniq ->
                            comma = if acc_uniq == "", do: "", else: ", "

                            column_name =
                              Column.sql_column_name(context, str_node_name_key, column_name)

                            "#{acc_uniq}#{comma}#{column_name}"
                          end)

                        temp_sql = "COUNT(#{distinct_sql})"
                        %{"sql_acc" => temp_sql, "joins" => joins_acc }
                      key in ["dao@average", "dao@avg"] ->
                        distinct_sql =
                          Enum.reduce(format_config, "", fn column_name, acc_uniq ->
                            comma = if acc_uniq == "", do: "", else: ", "

                            column_name =
                              Column.sql_column_name(context, str_node_name_key, column_name)

                            "#{acc_uniq}#{comma}#{column_name}"
                          end)

                        temp_sql = "AVG(#{distinct_sql})"
                        %{"sql_acc" => temp_sql, "joins" => joins_acc }
                      key in ["dao@total", "dao@sum"] ->
                        distinct_sql =
                          Enum.reduce(format_config, "", fn column_name, acc_uniq ->
                            comma = if acc_uniq == "", do: "", else: ", "

                            column_name =
                              Column.sql_column_name(context, str_node_name_key, column_name)

                            "#{acc_uniq}#{comma}#{column_name}"
                          end)

                        temp_sql = "SUM(#{distinct_sql})"
                        %{"sql_acc" => temp_sql, "joins" => joins_acc }
                      true ->
                        # nyd: take care of other scenarios
                        %{"sql_acc" => sql_acc, "joins" => joins_acc }
                    end

                  true ->
                    # check for parent tables and joins
                    is_propbably_ajoin_term = Column.is_propbably_ajoin_term(format_config)
                    # sql_acc  =
                    if is_propbably_ajoin_term do
                      join_table_key = String.to_atom(key)

                      xresult =
                        gen_sql_for_get_fixture_helper(
                          result_acc,
                          join_table_key,
                          format_config,
                          true
                        )

                      {base_col_name, embed_col_name, join_type} =
                        if is_map(format_config) && (Map.has_key?(format_config, "dao@link") || Map.has_key?(format_config, "dao@join")) do
                          join_conf =
                          if Map.has_key?(format_config, "dao@link") do
                            format_config["dao@link"]
                          else
                            format_config["dao@join"]
                          end
                          case join_conf do
                            {bcn, ecn} -> {bcn, ecn, "LEFT"}
                            {bcn, ecn, jtype} ->
                              jtype = jtype |> String.trim() |> String.upcase()
                              #OUTER is full outer join
                              if jtype in ["LEFT", "RIGHT", "OUTER", "INNER"] do
                                {bcn, ecn, jtype}
                              else
                                throw("Unkown join type " <> jtype)
                              end
                          end
                        else
                          {"", "", "LEFT"}
                        end

                      sqlx = xresult["fixture_list"][join_table_key]["sql"]
                      #example FROM employee JOIN branch ON employee.emp_id = branch.mgr_id
                      join_plural_table_key = Inflex.pluralize(key)
                      join_sql_table_name =  Table.sql_table_name(context, join_plural_table_key)

                      join_sql =
                      if base_col_name != "" do
                        #this means the linkage was explicitly scpecified {xx, yy}
                        select_table_column_name = Table.sql_table_column_name(context, str_node_name_key, base_col_name)
                        join_table_column_name = Table.sql_table_column_name(context, join_plural_table_key, embed_col_name)
                        " #{join_type} JOIN #{join_sql_table_name} ON #{select_table_column_name} = #{join_table_column_name}"
                      else
                        str_node_plural_name_key = Inflex.pluralize(str_node_name_key)
                        {fk_name, fk_col_config} = Column.get_foreign_key_config(context["schema"][str_node_plural_name_key], join_plural_table_key)
                        select_table_column_name = Table.sql_table_column_name(context, str_node_name_key, fk_name)
                        link_col_name = fk_col_config["on"]
                        join_table_column_name = Table.sql_table_column_name(context, join_plural_table_key, link_col_name)
                        " #{join_type} JOIN #{join_sql_table_name} ON #{select_table_column_name} = #{join_table_column_name}"
                      end

                      parent_sql = String.trim(sqlx["select"])
                      comma = if sql_acc == "", do: "", else: ", "
                      temp_sql = "#{sql_acc}#{comma}#{parent_sql}"
                      joins = sqlx["joins"] ++ [join_sql]
                      %{"sql_acc" => temp_sql, "joins" => joins_acc ++ joins }
                    else
                      comma = if sql_acc == "", do: "", else: ", "
                      column_name = Column.sql_column_name(context, str_node_name_key, key)

                      column_name =
                        cond do
                          is_map(format_config) && Map.has_key?(format_config, "as") ->
                            "#{column_name} AS #{format_config["as"]}"

                          is_binary(format_config) && Column.is_data_type(format_config) == false ->
                            # nyd: this will be used as the as shorthand for now
                            "#{column_name} AS #{format_config}"

                          true ->
                            column_name
                        end

                      temp_sql = "#{sql_acc}#{comma}#{column_name}"
                      %{"sql_acc" => temp_sql, "joins" => joins_acc}
                    end
                end
              end)
          end

        specififc_cols_sql =
          if String.trim(specififc_cols_sql) == "", do: "*", else: specififc_cols_sql

        where_sql =
          if is_map(query_config) do
            where_clause_config = Map.get(query_config, "dao@where")

            where_sql = Operator.process_where_clause(context, query_config, where_clause_config)

            where_sql = String.trim(where_sql)
            # this applies only if timestamps have not been turned off in the schema
            and_condition = if where_sql == "", do: "", else: "(#{where_sql}) AND "
            " WHERE #{and_condition}is_deleted = 0"
          else
            " WHERE is_deleted = 0"
          end

        order_by_sql = OrderBy.gen_sql(context, query_config)
        group_by_sql = GroupBy.gen_sql(context, query_config)
        pagination_sql = Pagination.gen_sql(context, query_config)

        joins_sql =
        if length(joins_list) > 0 do
          # IO.inspect(joins_list)
          # jsql = " LEFT JOIN " <> Enum.join(joins_list, " LEFT JOIN ")
          Enum.join(joins_list, "")
        else
          ""
        end

        select_table_name = Table.sql_table_name(result_acc["context"], str_node_name_key)

        if return_parts == true do
          %{
            "select" => specififc_cols_sql,
            "from" => select_table_name,
            "where" => where_sql,
            "orderby" => order_by_sql,
            "groupby" => group_by_sql,
            "pagination" => pagination_sql,
            "joins" => joins_list
          }
        else
          "#{sql} #{specififc_cols_sql} FROM #{select_table_name}#{joins_sql}#{where_sql}#{order_by_sql}#{group_by_sql}#{pagination_sql}"
        end
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

        %{
          "context" => context,
          "fixture_list" => [{node_name_key, fixture_config} | result_acc["fixture_list"]]
        }

      insert_sql == "" && any_alter_table_sql != "" ->
        # there is no data to be updated, but we have some schema changes
        # then these are gona be fixture because they are the only queries in the root command
        fixture_config = %{
          "sql" => any_alter_table_sql,
          "is_list" => is_list
        }

        %{
          "context" => context,
          "fixture_list" => [{node_name_key, fixture_config} | result_acc["fixture_list"]]
        }

      insert_sql == "" && any_alter_table_sql == "" ->
        fixture_config = %{
          "sql" => "",
          "is_list" => is_list
        }

        %{
          "context" => context,
          "fixture_list" => [{node_name_key, fixture_config} | result_acc["fixture_list"]]
        }
    end
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

      if is_list(query_config) do
        # the current implementation when the query_config is a list, is that we have an array of update maps
        list_acc = %{
          "context" => context,
          "fixture_list" => [
            {node_name_key,
             %{
               "is_list" => is_list,
               "sql" => []
             }}
          ]
        }

        # excute the in a loop
        Enum.reduce(query_config, list_acc, fn query_config_item, acc ->
          %{"context" => context, "fixture_list" => temp_results} =
            gen_sql_for_update_fixture_helper(acc, node_name_key, query_config_item, is_list)

          %{"is_list" => temp_is_list, "sql" => temp_sql} =
            Keyword.get(temp_results, node_name_key, %{"is_list" => false, "sql" => ""})

          %{"is_list" => _acc_is_list, "sql" => acc_sql_list} =
            Keyword.get(acc["fixture_list"], node_name_key)

          %{
            "context" => context,
            "fixture_list" => [
              {node_name_key,
               %{
                 "is_list" => temp_is_list,
                 "sql" => acc_sql_list ++ [temp_sql]
               }}
            ]
          }
        end)
      else
        gen_sql_for_update_fixture_helper(result_acc, node_name_key, query_config, is_list)
      end
    end)
  end

  def gen_sql_for_update_fixture_helper(result_acc, node_name_key, query_config, is_list) do
    str_node_name_key = Atom.to_string(node_name_key)
    # check if the table and its cols exist in the schema
    ensure_table_response =
      Table.gen_sql_ensure_table_exists(result_acc["context"], str_node_name_key, query_config)

    # Utils.log("ensure_table_exist", ensure_table_response)

    # {context, any_alter_table_sql} =
    #   case ensure_table_response do
    #     {context, sql} -> {context, sql}
    #     context -> {context, ""}
    #   end
    {context, any_alter_table_sql, is_def_only} =
      case ensure_table_response do
        {context, sql} -> {context, sql, true}
        {context, sql, is_def_only} -> {context, sql, is_def_only}
        context -> {context, "", true}
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
                skip = Utils.skip_keys()

                if key in skip do
                  sql_acc
                else
                  comma = if sql_acc == "", do: "", else: ", "
                  str_val = Column.sql_value_format(value)
                  "#{sql_acc}#{comma}#{key} = #{str_val}"
                end
              end)

            where_clause_config = Map.get(query_config, "dao@where")

            where_sql = Operator.process_where_clause(context, query_config, where_clause_config)

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

        %{
          "context" => context,
          "fixture_list" => [{node_name_key, fixture_config} | result_acc["fixture_list"]]
        }

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

        %{
          "context" => context,
          "fixture_list" => [{node_name_key, fixture_config} | result_acc["fixture_list"]]
        }
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

          %{
            "context" => new_context,
            "fixture_list" => [{node_name_key, fixture_config} | result_acc["fixture_list"]]
          }

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
            "fixture_list" => [{node_name_key, fixture_config} | result_acc["fixture_list"]]
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

          where_sql =
            Operator.process_where_clause(result_acc["context"], query_config, where_config)

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
            "fixture_list" => [{node_name_key, fixture_config} | result_acc["fixture_list"]]
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

          %{
            "context" => new_context,
            "fixture_list" => [{node_name_key, fixture_config} | result_acc["fixture_list"]]
          }

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

  @spec load_config_from_file(
          binary
          | maybe_improper_list(
              binary | maybe_improper_list(any, binary | []) | char,
              binary | []
            )
        ) :: {:ok, map} | {:error, <<_::64, _::_*8>>, atom | Jason.DecodeError.t()}
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
