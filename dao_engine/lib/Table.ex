defmodule Table do
  @spec gen_sql_ensure_table_exists(map(), binary(), map()) :: map() | {map(), binary()}
  def gen_sql_ensure_table_exists(
        %{"auto_alter_db" => false} = context,
        _table_name,
        _query_config
      ),
      do: context

  def gen_sql_ensure_table_exists(context, table_name, query_config) do
    plural_table_name = Inflex.pluralize(table_name)

    if context["schema"] |> Map.has_key?(plural_table_name) == false do
      query_config = preprocess_query_config(context, query_config)
      Utils.log("checkout", query_config, plural_table_name == "employeexes")
      gen_sql_table(context, plural_table_name, query_config)
    else
      # ensure that it has all the columns specified in the query config
      proc_query_config =
        cond do
          is_list(query_config) ->
            query_config_from_list =
              Column.list_to_query_config(context, plural_table_name, query_config)

            preprocess_query_config(context, query_config_from_list)

          is_map(query_config) ->
            preprocess_query_config(context, query_config)

          true ->
            throw("Unknown query config: ensuring all columns exist")
        end

      # Utils.log("proc_query_config", proc_query_config, is_list(query_config))
      gen_columns_sql = Column.gen_sql_columns(context, plural_table_name, proc_query_config)
      # Utils.log("gen_columns_sql", gen_columns_sql, is_list(query_config))
      gen_sql_cols(context, plural_table_name, gen_columns_sql)
    end
  end

  def gen_sql_table(context, plural_table_name, query_config) do
    set_default_standard_pk =
      if Map.has_key?(query_config, "use_default_pk"),
        do: query_config["use_default_pk"],
        else: true

    default_standard_col_def_pk =
      if set_default_standard_pk == true do
        definition = Column.define_column(context, "id", "pk")
        %{definition | "sql" => definition["sql"]}
      else
        %{
          "sql" => "",
          "config" => %{}
        }
      end

    primary_keys_line =
      Column.define_columnx(context, "use_primary_keys", query_config["use_primary_keys"])

    table_schema =
      if set_default_standard_pk == true do
        Map.put(%{}, "id", default_standard_col_def_pk["config"])
      else
        %{}
      end

    table_col_def =
      if Map.has_key?(query_config, "columns") == true do
        acc = %{
          "sql" => "",
          "table_schema" => %{},
          "is_def_only" => true,
          "foreign_keys_sql" => []
        }

        Enum.reduce(query_config["columns"], acc, fn {column_name_key, column_config}, acc ->
          skips = Utils.skip_keys()

          if column_name_key in skips do
            # just continue
            acc
          else
            sql_acc = String.trim(acc["sql"])
            table_schema = acc["table_schema"]
            col_def = Column.define_column(context, column_name_key, column_config)
            # update the schema
            schema = Map.put(table_schema, column_name_key, col_def["config"])
            comma = if sql_acc == "", do: "", else: ", "
            sql = sql_acc <> comma <> String.trim(col_def["sql"])
            # check if we have a col def
            is_def_only =
              if acc["is_def_only"] == false,
                do: false,
                else: Map.get(col_def, "is_def_only", true)

            foreign_sql =
              if Map.has_key?(col_def["config"], "fk") do
                plural_parent_table_name =
                  col_def["config"] |> Map.get("fk") |> Inflex.pluralize()

                linked_column_name = column_config["on"]

                on_delete_sql =
                  if column_config["on_delete"] in [nil, "null", "NULL"],
                    do: " SET NULL",
                    else: " CASCADE"

                fsql =
                  "FOREIGN KEY(#{column_name_key}) REFERENCES #{plural_parent_table_name}(#{linked_column_name}) ON DELETE#{on_delete_sql}"

                acc["foreign_keys_sql"] ++ [fsql]
              else
                acc["foreign_keys_sql"]
              end

            %{
              "sql" => sql,
              "table_schema" => schema,
              "is_def_only" => is_def_only,
              "foreign_keys_sql" => foreign_sql
            }
          end
        end)
      else
        %{
          "sql" => "",
          "table_schema" => %{},
          "is_def_only" => true,
          "foreign_keys_sql" => []
        }
      end

    set_default_standard_timestamps =
      if Map.has_key?(query_config, "use_standard_timestamps") do
        query_config["use_standard_timestamps"]
      else
        # consult context
        use_standard_timestamps =
          if Map.has_key?(context, "dao@timestamps") do
            context["dao@timestamps"]
          else
            true
          end

        if Map.has_key?(context, "use_standard_timestamps") do
          context["use_standard_timestamps"]
        else
          use_standard_timestamps
        end
      end

    default_table_schema =
      if set_default_standard_timestamps == true do
        created_at_col_def = Column.define_column(context, "created_at", "datetime")

        last_update_on_col_def =
          Column.define_column(context, "last_update_on", %{
            "type" => "datetime",
            "default" => "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
          })

        is_deleted_col_def = Column.define_column(context, "is_deleted", "boolean")

        deleted_on_col_def =
          Column.define_column(context, "deleted_on", %{
            "type" => "datetime",
            "default" => "NULL",
            "required" => false
          })

        %{
          "table_schema" =>
            table_schema
            |> Map.put("created_at", created_at_col_def["config"])
            |> Map.put("last_update_on", last_update_on_col_def["config"])
            |> Map.put("is_deleted", is_deleted_col_def["config"])
            |> Map.put("deleted_on", deleted_on_col_def["config"]),
          "sql" =>
            Enum.join(
              [
                created_at_col_def["sql"],
                last_update_on_col_def["sql"],
                is_deleted_col_def["sql"],
                deleted_on_col_def["sql"]
              ],
              ", "
            )
        }
      else
        %{
          "table_schema" => table_schema,
          "sql" => ""
        }
      end

    foreign_keys_lines = table_col_def["foreign_keys_sql"] |> Enum.join(",")

    pk_sql = default_standard_col_def_pk["sql"]
    sql = "CREATE TABLE #{sql_table_name(context, plural_table_name)} (#{pk_sql}"

    table_col_def_sql = String.trim(table_col_def["sql"])
    default_table_schema_sql = default_table_schema["sql"]

    comma =
      if String.trim(default_table_schema_sql) == "" || String.trim(table_col_def_sql) == "",
        do: "",
        else: ", "

    first_comma = if String.trim(pk_sql) == "", do: "", else: ", "

    sql =
      sql <>
        first_comma <>
        String.trim(table_col_def_sql) <> comma <> String.trim(default_table_schema_sql)

    primary_keys_line_sql = primary_keys_line["sql"]
    comma = if String.trim(primary_keys_line_sql) == "", do: "", else: ", "
    sql = sql <> comma <> String.trim(primary_keys_line_sql)
    sql = String.trim(sql)
    sql = if String.ends_with?(sql, ","), do: String.trim_trailing(sql, ","), else: sql
    comma = if String.trim(foreign_keys_lines) == "", do: "", else: ", "
    sql = "#{sql}#{comma}#{foreign_keys_lines})"

    auto_schema_changes = context["auto_schema_changes"] ++ [sql]
    context = %{context | "auto_schema_changes" => auto_schema_changes}
    # update the schema

    table_schema = Map.merge(default_table_schema["table_schema"], table_col_def["table_schema"])

    schema = Map.put(context["schema"], plural_table_name, table_schema)
    {%{context | "schema" => schema}, "", table_col_def["is_def_only"]}
  end

  @spec gen_sql_cols(any, any, nil | maybe_improper_list | map) :: {any, binary}
  def gen_sql_cols(context, plural_table_name, generated_columns_sql) do
    # cbh
    sql = String.trim(generated_columns_sql["sql"])

    if sql != "" do
      sql = "ALTER TABLE #{sql_table_name(context, plural_table_name)} #{sql}"
      # this command must go into the automatic schema changes
      # it will be prefixed with dao@skip: so that it is not exectued twice, but will cause a migration to be recorded
      auto_schema_changes = context["auto_schema_changes"] ++ ["dao@skip: " <> sql]
      context = %{context | "auto_schema_changes" => auto_schema_changes}
      # Utils.log("generated_columns_sql", generated_columns_sql)
      schema =
        Map.put(context["schema"], plural_table_name, generated_columns_sql["table_schema"])

      context = %{context | "schema" => schema}
      {context, sql}
    else
      # nothing to do, because there are no new columns
      {context, ""}
    end
  end

  def sql_table_name(context, table_name) do
    plural_table_name = Inflex.pluralize(table_name)
    "`#{context["database_name"]}.#{plural_table_name}`"
  end

  def preprocess_query_config(context, config_table_def) when is_list(config_table_def) do
    preprocess_query_config(context, %{})
  end

  def preprocess_query_config(context, config_table_def) do
    # preprocess columns
    query_config = %{
      "columns" => %{}
    }

    # delete dao@def_only
    config_table_def = Map.delete(config_table_def, "dao@def_only")

    # nyd: please note that in this step the "is_list", flag doesnot have any effect at the moment
    {query_config, config_table_def} =
      Utils.ensure_key(
        config_table_def,
        "is_list",
        "is_list",
        nil,
        query_config
      )

    # consult context
    context_use_default_pk =
      if Map.has_key?(context, "dao@use_default_pk") do
        context["dao@use_default_pk"]
      else
        true
      end

    context_use_default_pk =
      if Map.has_key?(context, "use_default_pk") do
        context["use_default_pk"]
      else
        context_use_default_pk
      end

    {query_config, config_table_def} =
      Utils.ensure_key(
        config_table_def,
        "dao@use_default_pk",
        "use_default_pk",
        context_use_default_pk,
        query_config
      )

    # consult context
    context_use_standard_timestamps =
      if Map.has_key?(context, "dao@timestamps") do
        context["dao@timestamps"]
      else
        true
      end

    context_use_standard_timestamps =
      if Map.has_key?(context, "use_standard_timestamps") do
        context["use_standard_timestamps"]
      else
        context_use_standard_timestamps
      end

    {query_config, config_table_def} =
      Utils.ensure_key(
        config_table_def,
        "dao@timestamps",
        "use_standard_timestamps",
        context_use_standard_timestamps,
        query_config
      )

    # nyd: also consider supporting the using dao@primary_keys
    {query_config, config_table_def} =
      Utils.ensure_key(
        config_table_def,
        "dao@pks",
        "use_primary_keys",
        [],
        query_config
      )

    {query_config, config_table_def} =
      Utils.ensure_key(
        config_table_def,
        "dao@fks",
        "use_foreign_keys",
        [],
        query_config
      )

    {query_config, config_table_def} =
      Utils.ensure_key(
        config_table_def,
        "dao@skip_insert",
        "use_skip_insert",
        [],
        query_config
      )

    # copy columns over
    preprocess_config_table_def =
      if Map.has_key?(config_table_def, "columns") do
        %{query_config | "columns" => config_table_def["columns"]}
      else
        Enum.reduce(config_table_def, query_config, fn {node_key, node_value}, acc_query_config ->
          skip = Utils.skip_keys()

          if node_key in skip do
            acc_query_config
          else
            columns = Map.put(acc_query_config["columns"], node_key, node_value)
            %{acc_query_config | "columns" => columns}
          end
        end)
      end

    {context, preprocess_config_table_def} =
      OrderBy.preprocess_config(context, preprocess_config_table_def, config_table_def)

    {context, preprocess_config_table_def} =
      Pagination.preprocess_config(context, preprocess_config_table_def, config_table_def)

    preprocess_config_table_def
  end

  @spec get_table_structure_from_db :: nil
  def get_table_structure_from_db() do
    # nyd: this function may use a genserver cache for this or query agains the db directly
    # DESCRIBE student;
  end

  @spec get_sql_remove_table(map(), binary()) :: {map(), binary()}
  def get_sql_remove_table(context, table_name) do
    # please note that the setting "auto_alter_db" => true,
    # doesnot afftect this because this is done via a query
    # nyd: the only thing that can prevent this is authenticatiion

    # nyd: must do staff with the conextext and fixtures and schema, also think about migrations
    # nyd: how does this affect our relationships, foreing keys etc,
    # nyd: some of these commands may be solved over a long period of time
    db_table_name = sql_table_name(context, table_name)
    sql = "DROP TABLE #{db_table_name}"
    # remove this table from the schema
    schema = Map.delete(context["schema"], table_name)
    context = %{context | "schema" => schema}
    # this command must go into the automatic schema changes
    # it will be prefixed with dao@skip: so that it is not exectued twice, but will cause a migration to be recorded
    auto_schema_changes = context["auto_schema_changes"] ++ ["dao@skip: " <> sql]
    context = %{context | "auto_schema_changes" => auto_schema_changes}
    {context, sql}
  end

  @spec get_sql_add_col(map(), binary()) :: {map(), binary()}
  def get_sql_add_col(context, table_name) do
    table_name = sql_table_name(context, table_name)
    # please note that the setting "auto_alter_db" => true,
    # doesnot afftect this because this is done via a query
    # nyd: the only thing that can prevent this is authenticatiion

    # nyd: must do staff with the conextext and fixtures and schema, also think about migrations
    # nyd: how does this affect our relationships, foreing keys etc,
    # nyd: some of these commands may be solved over a long period of time
    table_name = sql_table_name(context, table_name)
    sql = "ALTER TABLE #{table_name} ADD "
    # remove this table from the schema
    schema = Map.delete(context["schema"], table_name)
    context = %{context | "schema" => schema}
    # this command must go into the automatic schema changes
    # it will be prefixed with dao@skip: so that it is not exectued twice, but will cause a migration to be recorded
    auto_schema_changes = context["auto_schema_changes"] ++ ["dao@skip: " <> sql]
    context = %{context | "auto_schema_changes" => auto_schema_changes}
    {context, sql}
  end

  @spec get_sql_remove_columns(map(), binary(), []) :: {map(), binary()}
  def get_sql_remove_columns(context, table_name, query_config) do
    # please note that the setting "auto_alter_db" => true,
    # doesnot afftect this because this is done via a query
    # nyd: the only thing that can prevent this is authenticatiion

    # nyd: must do staff with the conextext and fixtures and schema, also think about migrations
    # nyd: how does this affect our relationships, foreing keys etc,
    # nyd: some of these commands may be solved over a long period of time
    db_table_name = sql_table_name(context, table_name)
    # remove this columns from the table in the schema
    {context, sql} =
      if Map.has_key?(context["schema"], table_name) do
        acc = %{
          "table_schema" => context["schema"][table_name],
          "sql" => "",
          "errors" => %{}
        }

        process_results =
          Enum.reduce(query_config, acc, fn column_name, acc ->
            sql_acc = String.trim(acc["sql"])
            table_schema = acc["table_schema"]
            comma = if sql_acc == "", do: "", else: ", "
            sql = sql_acc <> comma <> column_name
            # nyd: throw an eeor here, attempting to delete a cloumn that does not exist
            table_schema = Map.delete(table_schema, column_name)
            %{"sql" => sql, "table_schema" => table_schema}
          end)

        schema = Map.put(context["schema"], table_name, process_results["table_schema"])
        context = %{context | "schema" => schema}
        sql = "ALTER TABLE #{db_table_name} DROP COLUMN " <> process_results["sql"]
        {context, sql}
      else
        # nyd: throw an eeor here, attempting to delete from a table that does not exist
        {context, ""}
      end

    # this command must go into the automatic schema changes
    # it will be prefixed with dao@skip: so that it is not exectued twice, but will cause a migration to be recorded
    auto_schema_changes = context["auto_schema_changes"] ++ ["dao@skip: " <> sql]
    context = %{context | "auto_schema_changes" => auto_schema_changes}
    {context, sql}
  end
end
