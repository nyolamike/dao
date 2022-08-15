defmodule DaoEngine do
  @moduledoc """
  Documentation for `DaoEngine`.
  """

  import Inflex

  alias DaoEngine, as: Dao

  @doc """
  Hello world.

  ## Examples

      iex> DaoEngine.hello()
      :world

  """
  def hello do
    :world
  end

  @doc """
    %{
      get: {
        list_of: %{
          shops: %{}
        },
      }
    }
  """

  def execute(context, query_object) do
    # query object have the following root keys
    # get
    # add
    # edit
    # delete
    Enum.map(query_object, fn {command_key, input_kwl_node} ->
      case command_key do
        :get ->
          gen_sql_for_get(context, query_object, input_kwl_node)

        _ ->
          "UNKNOWN"
      end
    end)
  end

  def gen_sql_for_get(context, query_object, input_kwl_node) do
    Enum.map(input_kwl_node, fn {node_name_key, fixtures_kwl_node} ->
      fixtures = gen_sql_for_get_fixture(context, query_object, fixtures_kwl_node)
      {node_name_key, fixtures}
    end)
  end

  def gen_sql_for_get_fixture(context, query_object, fixtures_kwl_node) do
    Enum.map(fixtures_kwl_node, fn {node_name_key, query_config} ->
      str_node_name_key = Atom.to_string(node_name_key)
      is_list = is_word_plural?(query_config, str_node_name_key)
      plural_table_name = Inflex.pluralize(str_node_name_key)
      sql = "SELECT * "
      sql = sql <> " FROM `" <> context.database_name <> "." <> plural_table_name  <> "`"
      sql = sql <> " WHERE is_deleted == 0 "
      fixture_config = %{
        sql: sql,
        is_list: is_list
      }

      {node_name_key, fixture_config}
    end)
  end

  def is_word_plural?(query_config, word) do
    single_word = Inflex.singularize(word)
    plural_word = Inflex.pluralize(word)

    cond do
      single_word == plural_word ->
        # special kind of word, we look into the config
        if Map.has_key?(query_config, :is_list) do
          query_config.is_list == true
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

  def try() do
    context = %{
      database_name: "grocerify"
    }

    query = [
      get: [
        list_of_shops: [
          # a fixture
          shops: %{}
        ]
      ],
      get: [
        keys: [
          aircraft: %{
            is_list: false
          }
        ]
      ],
      wedontknoe: false
    ]

    execute(context, query)
  end
end
