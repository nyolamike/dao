defmodule Database do
  def gen_sql_ensure_database_exists(context) do
    case check_database_exists_and_strutrue(context) do
      {:ok, false, _current_schema} ->
        gen_sql_create_db(context)

      {:ok, true, _current_schema} ->
        # nyd: vaidate that the schema is uptodate with the real database schema
        context

      error ->
        error
    end
  end

  def gen_sql_create_db(context) do
    db_name = context["database_name"]
    create_db_sql = "CREATE DATABASE IF NOT EXISTS #{db_name}"
    auto_changes = context["auto_schema_changes"]

    context =
      if create_db_sql in auto_changes do
        context
      else
        auto_changes = auto_changes ++ [create_db_sql]
        %{context | "auto_schema_changes" => auto_changes}
      end

    # validate the schema
    with {:ok, valid_schema} <- validate_schema(context) do
      %{"context" => context, "fixture_list" => _} =
        gen_sql_for_create_table_fixture(context, valid_schema)

      context
    end
  end

  def gen_sql_for_create_table_fixture(context, valid_schema) do
    result = %{
      "context" => context,
      "fixture_list" => []
    }

    Enum.reduce(valid_schema, result, fn {config_plural_table_name, config_table_def},
                                         result_acc ->
      # preprocess columns
      preprocess_config_table_def =
        Table.preprocess_query_config(result_acc["context"], config_table_def)

      context =
        Table.gen_sql_table(
          result_acc["context"],
          config_plural_table_name,
          preprocess_config_table_def
        )

      %{"context" => context, "fixture_list" => []}
    end)
  end

  def check_database_exists_and_strutrue(context) do
    # nyd: check if db exist, we could rely on a project_name_slug genserver for this, so that
    # we dont have to query the actually db for this for all queries
    db_name = context["database_name"]

    check_sql =
      "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '#{db_name}'"

    with {:ok, dms_res} <- MyXQL.query(:myxql, check_sql) do
      num_rows = dms_res.num_rows

      if num_rows == 0 do
        {:ok, false, nil}
      else
        {:ok, true, nil}
      end
    else
      _ -> {:error, "Some thing wieard happened"}
    end
  end

  def gen_sql_for_drop_db(context) do
    db_name = context["database_name"]
    create_db_sql = "DROP DATABASE IF EXISTS #{db_name}"
    auto_changes = context["auto_schema_changes"] ++ [create_db_sql]
    %{context | "auto_schema_changes" => auto_changes}
  end

  def gen_sql_for_reset_db(context) do
    # we drop the db and recreate it
    context = gen_sql_for_drop_db(context)
    gen_sql_create_db(context)
  end

  def validate_schema(context) do
    # nyd: table names must be plural etc
    {:ok, context["schema"]}
  end
end
