defmodule Table do
  @spec gen_sql_ensure_table_exists(map(), binary(), map()) :: map() | { map(), binary()}
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
      gen_sql_table(context, table_name, query_config)
    else
      # ensure that it has all the columns specified in the query config
      query_config = preprocess_query_config(context, query_config)
      #Utils.log("query_config", query_config)
      gen_columns_sql = Column.gen_sql_columns(context, plural_table_name, query_config)
      # Utils.log("gen_columns_sql", gen_columns_sql)
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
          "table_schema" => %{}
        }

        Enum.reduce(query_config["columns"], acc, fn {column_name_key, column_config}, acc ->
          sql_acc = String.trim(acc["sql"])
          table_schema = acc["table_schema"]
          col_def = Column.define_column(context, column_name_key, column_config)
          # update the schema
          schema = Map.put(table_schema, column_name_key, col_def["config"])
          comma = if sql_acc == "", do: "", else: ", "
          sql = sql_acc <> comma <> String.trim(col_def["sql"])
          %{"sql" => sql, "table_schema" => schema}
        end)
      else
        %{
          "sql" => "",
          "table_schema" => %{}
        }
      end

    set_default_standard_timestamps =
      if Map.has_key?(query_config, "use_standard_timestamps") do
        query_config["use_standard_timestamps"]
      else
        true
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

    sql =
      "CREATE TABLE #{sql_table_name(context, plural_table_name)} (#{default_standard_col_def_pk["sql"]}"

    table_col_def_sql = String.trim(table_col_def["sql"])
    default_table_schema_sql = default_table_schema["sql"]
    comma = if String.trim(default_table_schema_sql) == "", do: "", else: ", "
    sql = sql <> table_col_def_sql <> comma <> default_table_schema_sql
    primary_keys_line_sql = primary_keys_line["sql"]
    comma = if String.trim(primary_keys_line_sql) == "", do: "", else: ", "
    sql = sql <> comma <> primary_keys_line_sql
    sql = sql <> ")"

    auto_schema_changes = context["auto_schema_changes"] ++ [sql]
    context = %{context | "auto_schema_changes" => auto_schema_changes}
    # update the schema

    table_schema = Map.merge(default_table_schema["table_schema"], table_col_def["table_schema"])

    schema = Map.put(context["schema"], plural_table_name, table_schema)
    %{context | "schema" => schema}
  end

  def gen_sql_cols(context, plural_table_name, generated_columns_sql) do
    # cbh
    sql = String.trim(generated_columns_sql["sql"])

    if sql != "" do
      sql = "ALTER TABLE #{sql_table_name(context, plural_table_name)} #{sql}"
      # this command must go into the automatic schema changes
      # it will be prefixed with dao@skip: so that it is not exectued twice, but will cause a migration to be recorded
      auto_schema_changes = context["auto_schema_changes"] ++ ["dao@skip: " <> sql]
      context = %{context | "auto_schema_changes" => auto_schema_changes}
      #Utils.log("generated_columns_sql", generated_columns_sql)
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

  def preprocess_query_config(context, config_table_def) do
    # preprocess columns
    query_config = %{
      "columns" => %{}
    }

    # nyd: please note that in this step the "is_list", flag doesnot have any effect at the moment
    {query_config, config_table_def} =
      Utils.ensure_key(
        config_table_def,
        "is_list",
        "is_list",
        nil,
        query_config
      )

    {query_config, config_table_def} =
      Utils.ensure_key(
        config_table_def,
        "dao@use_default_pk",
        "use_default_pk",
        true,
        query_config
      )

    {query_config, config_table_def} =
      Utils.ensure_key(
        config_table_def,
        "dao@timestamps",
        "use_standard_timestamps",
        true,
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
          columns = Map.put(acc_query_config["columns"], node_key, node_value)
          %{acc_query_config | "columns" => columns}
        end)
      end

    preprocess_config_table_def
  end

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

  # nyd ALTER TABLE student DROP COLUMN gpa;
end
