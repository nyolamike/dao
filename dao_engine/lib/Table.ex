defmodule Table do
  @spec gen_sql_ensure_table_exists(map(), binary(), map()) :: map()
  def gen_sql_ensure_table_exists(
        %{"auto_alter_db" => false} = context,
        _table_name,
        _query_config
      ),
      do: context

  def gen_sql_ensure_table_exists(context, table_name, query_config) do
    plural_table_name = Inflex.pluralize(table_name)

    if context["schema"] |> Map.has_key?(plural_table_name) == false do
      gen_sql_table(context, table_name, query_config)
    else
      context
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
    sql = sql <> table_col_def_sql <> comma <> default_table_schema_sql <> ")"

    auto_schema_changes = context["auto_schema_changes"] ++ [sql]
    context = %{context | "auto_schema_changes" => auto_schema_changes}
    # update the schema

    table_schema = Map.merge(default_table_schema["table_schema"], table_col_def["table_schema"])

    schema = Map.put(context["schema"], plural_table_name, table_schema)
    %{context | "schema" => schema}
  end

  def sql_table_name(context, table_name) do
    plural_table_name = Inflex.pluralize(table_name)
    "`#{context["database_name"]}.#{plural_table_name}`"
  end
end
