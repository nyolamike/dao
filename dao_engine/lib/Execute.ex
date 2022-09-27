defmodule Execute do
  def auto_schema_changes(gen_sql_res) do
    context = gen_sql_res["context"]
    schema_changes = Map.get(context, "auto_schema_changes", [])

    cummulator = %{
      "index_ref_results" => %{},
      "next_index" => 0,
      "context" => context,
      "connections" => %{},
      "errors" => %{},
      # nyd: implement has_errors
      "has_errors" => false
    }

    schema_change_results =
      Enum.reduce_while(schema_changes, cummulator, fn sql, acc ->
        results_bucket = acc["index_ref_results"]
        context = acc["context"]
        current_index = acc["next_index"]
        connections = acc["connections"]
        db_name = context["database_name"]

        {connections, query_results} =
          query_ensure_connectioned(context, sql, db_name, connections)

        results_temp = %{
          "sql" => sql,
          "results" => query_results
        }

        results_bucket = Map.put(results_bucket, current_index, results_temp)

        acc = %{
          acc
          | "next_index" => current_index + 1,
            "index_ref_results" => results_bucket,
            "connections" => connections
        }

        {:cont, acc}
      end)

    results = %{
      "index_ref_results" => schema_change_results["index_ref_results"],
      "next_index" => schema_change_results["next_index"],
      "errors" => schema_change_results["errors"],
      "has_errors" => schema_change_results["has_errors"]
    }

    gen_sql_res
    |> Map.put("context", schema_change_results["context"])
    |> Map.put("connections", schema_change_results["connections"])
    |> Map.put("auto_schema_changes_results", results)
  end

  def root_cmd_sqls(gen_sql_schema_results) do
    context = gen_sql_schema_results["context"]
    connections = gen_sql_schema_results["connections"]
    root_cmd_node_list = Map.get(gen_sql_schema_results, "root_cmd_node_list", [])
    # IO.inspect(root_cmd_node_list)
    # remember that these need to be reversed before processing them
    root_cmd_node_list = Enum.reverse(root_cmd_node_list)

    root_cmd_cummulator = %{
      "results" => [],
      "context" => context,
      "connections" => connections,
      "errors" => %{},
      # nyd: implement has_errors
      "has_errors" => false
    }

    root_cmd_results =
      Enum.reduce_while(root_cmd_node_list, root_cmd_cummulator, fn {root_cmd_node_name_key,
                                                                     named_kwl_queries},
                                                                    acc ->
        context = acc["context"]
        connections = acc["connections"]
        current_rsults = acc["results"]
        errors = acc["errors"]
        has_errors = acc["has_errors"]

        named_cummulator = %{
          "results" => [],
          "context" => context,
          "connections" => connections,
          "errors" => %{},
          # nyd: implement has_errors
          "has_errors" => false
        }

        named_results =
          Enum.reduce_while(named_kwl_queries, named_cummulator, fn temp_node, inner_acc ->
            {name_of_query_key, list_for_the_named_query} = temp_node

            inn_context = inner_acc["context"]
            inn_connections = inner_acc["connections"]
            inn_current_rsults = inner_acc["results"]
            inn_errors = inner_acc["errors"]
            inn_has_errors = inner_acc["has_errors"]

            several_manipulations_cummulator = %{
              "results" => [],
              "context" => inn_context,
              "connections" => inn_connections,
              "errors" => %{},
              # nyd: implement has_errors in the logic below
              "has_errors" => false
            }

            manipulations_results =
              Enum.reduce_while(list_for_the_named_query, several_manipulations_cummulator, fn qn,
                                                                                               query_acc ->
                {table_key, sql_map} = qn

                qacc_context = query_acc["context"]
                qacc_connections = query_acc["connections"]
                qacc_current_rsults = query_acc["results"]
                qacc_errors = query_acc["errors"]
                qacc_has_errors = query_acc["has_errors"]

                sql = sql_map["sql"]

                db_name = qacc_context["database_name"]
                # nyd: how about where sql is a list
                {connections, query_results} =
                  query_ensure_connectioned(qacc_context, sql, db_name, qacc_connections)

                results_temp = %{
                  "sql" => sql,
                  "results" => query_results
                }

                new_result = [{table_key, results_temp} | qacc_current_rsults]

                query_acc = %{
                  "results" => new_result,
                  "context" => qacc_context,
                  "connections" => connections,
                  # implement logic of getting errors
                  "errors" => Map.merge(qacc_errors, %{}),
                  "has_errors" => false || qacc_has_errors
                }

                {:cont, query_acc}
              end)

            res_mani_temp = [
              {name_of_query_key, manipulations_results["results"]} | inn_current_rsults
            ]

            inner_acc = %{
              "results" => res_mani_temp,
              "context" => manipulations_results["context"],
              "connections" => manipulations_results["connections"],
              "errors" => Map.merge(inn_errors, manipulations_results["errors"]),
              "has_errors" => manipulations_results["has_errors"] || inn_has_errors
            }

            {:cont, inner_acc}
          end)

        input_node_list = [{root_cmd_node_name_key, named_results["results"]} | current_rsults]

        acc = %{
          "results" => input_node_list,
          "context" => named_results["context"],
          "connections" => named_results["connections"],
          "errors" => Map.merge(errors, named_results["errors"]),
          "has_errors" => named_results["has_errors"] || has_errors
        }

        {:cont, acc}
      end)

    results = %{
      "results" => root_cmd_results["results"],
      "errors" => root_cmd_results["errors"],
      "has_errors" => root_cmd_results["has_errors"]
    }

    gen_sql_schema_results
    |> Map.put("context", root_cmd_results["context"])
    |> Map.put("connections", root_cmd_results["connections"])
    |> Map.put("root_cmd_sqls_results", results)
  end

  def query_ensure_connectioned(context, sql_query, db_name, connections \\ %{}) do
    db_name = if is_atom(db_name), do: Atom.to_string(db_name), else: db_name

    {connections, query_results} =
      if String.starts_with?(sql_query, [
           "ALTER",
           "CREATE TABLE",
           "DROP TABLE",
           "SELECT",
           "DELETE"
         ]) do
        # requires a db connection
        # check if we already have a connection
        connections =
          if Map.has_key?(connections, db_name) == false do
            # try to connect
            conn_pid = Dbms.connect(context)
            Map.put(connections, db_name, conn_pid)
          else
            # nyd: post this "As dangerous as an if statement in elixir without and else cluase
            connections
          end

        connection = connections[db_name]

        # nyd: continue only if there are no errors
        # use the specific connction to the database
        query_res = Dbms.query(context, connection, sql_query)
        query_res = remove_connection_id(context, query_res)

        {connections, query_res}
      else
        # use the general connction to the dbms
        query_res = Dbms.query(context, sql_query)
        query_res = remove_connection_id(context, query_res)
        {connections, query_res}
      end

    {connections, query_results}
  end

  def remove_connection_id(context, {:ok, sql_result}) do
    remove_conn_ids = Map.get(context, "remove_sql_results_connection_ids", true)

    cleaned_result =
      if remove_conn_ids == true && is_map(sql_result) && Map.has_key?(sql_result, :connection_id) do
        Map.put(sql_result, :connection_id, :dao@removed)
      else
        sql_result
      end

    {:ok, cleaned_result}
  end

  def remove_connection_id(_context, sql_result), do: sql_result
end
