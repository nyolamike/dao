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

    Map.merge(gen_sql_res, schema_change_results)
  end

  def root_cmd_sqls(schema_change_results) do
    context = schema_change_results["context"]
    connections = schema_change_results["connections"]
    root_cmd_node_list = Map.get(schema_change_results, "root_cmd_node_list", [])
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
      Enum.reduce_while(root_cmd_node_list, root_cmd_cummulator, fn {root_cmd_node_name_key, named_kwl_queries}, acc ->

        context = acc["context"]
        connections = acc["connections"]
        current_rsults = acc["results"]

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

            current_rsults = inner_acc["results"]
            several_manipulations_cummulator = %{
              "results" => [],
              "context" => context,
              "connections" => connections,
              "errors" => %{},
              # nyd: implement has_errors in the logic below
              "has_errors" => false
            }
            manipulations_results =
              Enum.reduce_while(list_for_the_named_query, several_manipulations_cummulator, fn qn, query_acc ->
                {table_key, sql_map} = qn
                context = query_acc["context"]
                connections = query_acc["connections"]
                db_name = context["database_name"]
                sql = sql_map["sql"]
                current_results = query_acc["results"]

                # nyd: how about where sql is a list
                {connections, query_results} =
                  query_ensure_connectioned(context, sql, db_name, connections)

                results_temp = %{
                  "sql" => sql,
                  "results" => query_results
                }

                new_result = [{table_key, results_temp} | current_results]

                query_acc =%{query_acc | "context" => context, "connections" =>  connections, "results" => new_result}
                {:cont, query_acc}
              end)

            res_mani_temp = [{name_of_query_key, manipulations_results["results"]} | current_rsults]

            inner_acc = %{
              inner_acc
              | "context" => manipulations_results["context"],
                "connections" => manipulations_results["connections"],
                "results" => res_mani_temp
            }

            {:cont, inner_acc}
          end)

        input_node_list = [{root_cmd_node_name_key, named_results["results"]} | current_rsults]
        acc = %{"context" => context, "root_cmd_node_results_list" => input_node_list}
        {:cont, acc}
      end)

    # Utils.log("finde", root_cmd_results)


    Map.merge(schema_change_results, root_cmd_results)
  end

  def query_ensure_connectioned(context, sql_query, db_name, connections \\ %{}) do
    db_name = if is_atom(db_name), do: Atom.to_string(db_name), else: db_name
    {connections, query_results} =
      if String.starts_with?(sql_query, ["ALTER", "CREATE TABLE", "DROP TABLE", "SELECT", "DELETE"]) do
        # requires a db connection
        # check if we already have a connection
        connections =
          if Map.has_key?(connections, db_name) == false do
            # try to connect
            conn_pid = Dbms.connect(context)
            Map.put(connections, db_name, conn_pid)
          else
            #nyd: post this "As dangerous as an if statement in elixir without and else cluase
            connections
          end

        connection = connections[db_name]

        # nyd: continue only if there are no errors
        # use the specific connction to the database
        query_res = Dbms.query(context, connection, sql_query)
        {connections, query_res}
      else
        # use the general connction to the dbms
        query_res = Dbms.query(context, sql_query)
        {connections, query_res}
      end
    {connections, query_results}
  end
end
