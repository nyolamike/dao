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

    Enum.reduce(query_object, results, fn {command_key, input_kwl_node}, results_acc ->
      case command_key do
        :get ->
          %{"context" => context, "input_node_list" => input_node_list} =
            gen_sql_for_get(results_acc["context"], query_object, input_kwl_node)

          root_cmd_node_list = [{:get, input_node_list} | results_acc["root_cmd_node_list"]]
          %{"context" => context, "root_cmd_node_list" => root_cmd_node_list}

        _ ->
          "UNKNOWN"
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
      is_list = is_word_plural?(query_config, str_node_name_key)
      # check if the table exists in the schema
      context =
        Table.gen_sql_ensure_table_exists(result_acc["context"], str_node_name_key, query_config)

      sql = "SELECT *"
      sql = sql <> " FROM #{Table.sql_table_name(result_acc["context"], str_node_name_key)}"
      sql = sql <> " WHERE is_deleted == 0"

      fixture_config = %{
        "sql" => sql,
        "is_list" => is_list
      }

      %{"context" => context, "fixture_list" => [{node_name_key, fixture_config}]}
    end)
  end

  def is_word_plural?(query_config, word) do
    single_word = Inflex.singularize(word)
    plural_word = Inflex.pluralize(word)

    cond do
      single_word == plural_word ->
        # special kind of word, we look into the config
        if Map.has_key?(query_config, "is_list") do
          query_config["is_list"] == true
        else
          true
        end

      single_word == word ->
        false

      # plural_word == word
      true ->
        true
    end
  end

  def load_config_from_file(project_path_slug) do
    base_path = get_env("BASE_ROOT_FILE_PATH")
    path = Path.join([base_path, project_path_slug, "dao.json"])
    path = Path.expand(path)
    IO.inspect(path)

    with {:ok, body} <- File.read(path),
         {:ok, project_config} <- Jason.decode(body) do
      {:ok, project_config}
    else
      {:error, reason} ->
        {:error, "Failed to read database configuration file for project:#{project_path_slug}",
         reason}
    end
  end
end
