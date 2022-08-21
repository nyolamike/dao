defmodule Database do
  def gen_sql_ensure_database_exists(context) do
    case check_database_exists_and_strutrue(context) do
      {:ok, false, _current_schema} ->
        # validate the schema
        with {:ok, valid_schema} <- validate_schema(context) do
          %{"context" => context, "fixture_list" => _} =
            gen_sql_for_create_table_fixture(context, valid_schema)

          context
        end

      {:ok, true, _current_schema} ->
        :ok

      error ->
        error
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
      query_config = %{
        "columns" => %{}
      }

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

      preprocess_config_table_def =
        Enum.reduce(config_table_def, query_config, fn {node_key, node_value}, acc_query_config ->
          columns = Map.put(acc_query_config["columns"], node_key, node_value)
          %{acc_query_config | "columns" => columns}
        end)

      context =
        Table.gen_sql_table(
          result_acc["context"],
          config_plural_table_name,
          preprocess_config_table_def
        )

      %{"context" => context, "fixture_list" => []}
    end)
  end

  def check_database_exists_and_strutrue(_context) do
    # nyd: check if db exist, we could rely on a project_name_slug genserver for this, so that
    # we dont have to query the actually db for this for all queries
    {:ok, false, nil}
  end

  def validate_schema(context) do
    # nyd: table names must be plural
    {:ok, context["schema"]}
  end
end
