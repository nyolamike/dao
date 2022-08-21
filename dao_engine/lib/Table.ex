defmodule Table do

  import Inflex

  alias DaoEngine, as: Dao

  @default_table_schema %{
    "id" => "pk",
    "created_at" => "timestamp",
    "last_update_on" => "timestamp",
    "is_deleted" => "boolean",
    "deleted_on" => "timestamp"
  }

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
      set_default_standard_pk =
        if Map.has_key?(query_config, "use_default_pk"),
          do: query_config["use_default_pk"],
          else: true

      default_standard_col_def_pk =
        if set_default_standard_pk == true do
          definition = Column.define_column(context, "id", "pk")
          %{definition | "sql" => definition["sql"] <> ","}
        else
          %{
            "sql" => "",
            "config" => %{}
          }
        end

      table_col_def =
        if Map.has_key?(query_config, "columns") == true do
          acc = %{
            "sql" => "",
            "table_schema" => %{}
          }

          Enum.reduce(query_config["columns"], acc, fn {column_name_key, column_config},
                                                       %{
                                                         "sql" => sql_acc,
                                                         "table_schema" => table_schema
                                                       } ->
            col_def = Column.define_column(context, column_name_key, column_config)
            # update the schema
            schema = Map.put(table_schema, column_name_key, col_def["config"])
            comma = if sql_acc == "", do: "", else: ",\n "
            sql = sql_acc <> comma <> col_def["sql"]
            %{"sql" => sql, "table_schema" => schema}
          end)
        else
          %{"sql" => "", "table_schema" => @default_table_schema}
        end

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

      sql = "
          CREATE TABLE #{sql_table_name(context, plural_table_name)} (
          #{default_standard_col_def_pk["sql"]}
          #{table_col_def["sql"]},
          #{created_at_col_def["sql"]},
          #{last_update_on_col_def["sql"]},
          #{is_deleted_col_def["sql"]}
          #{deleted_on_col_def["sql"]}}
        )
      "

      auto_schema_changes = context["auto_schema_changes"] ++ [sql]
      context = %{context | "auto_schema_changes" => auto_schema_changes}
      # update the schema
      table_schema =
        if set_default_standard_pk == true do
          Map.put(%{}, "id", default_standard_col_def_pk["config"])
        else
          %{}
        end

      table_schema =
        table_schema
        |> Map.merge(table_col_def["table_schema"])
        |> Map.put("created_at", created_at_col_def["config"])
        |> Map.put("last_update_on", last_update_on_col_def["config"])
        |> Map.put("is_deleted", is_deleted_col_def["config"])
        |> Map.put("deleted_on", deleted_on_col_def["config"])

      schema = Map.put(context["schema"], plural_table_name, table_schema)
      %{context | "schema" => schema}
    else
      context
    end
  end

  def sql_table_name(context, table_name) do
    plural_table_name = Inflex.pluralize(table_name)
    "`#{context["database_name"]}.#{plural_table_name}`"
  end
end
