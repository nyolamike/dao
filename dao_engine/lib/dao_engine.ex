defmodule DaoEngine do
  @moduledoc """
  Documentation for `DaoEngine`.
  """

  import Inflex

  alias DaoEngine, as: Dao

  @default_table_schema %{
    id: :pk,
    created_at: :timestamp,
    last_update_on: :timestamp,
    is_deleted: :boolean,
    deleted_on: :timestamp
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
      context: context,
      root_cmd_node_list: []
    }

    Enum.reduce(query_object, results, fn {command_key, input_kwl_node}, results_acc ->
      case command_key do
        :get ->
          %{context: context, input_node_list: input_node_list} =
            gen_sql_for_get(results_acc.context, query_object, input_kwl_node)

          root_cmd_node_list = [{:get, input_node_list} | results_acc.root_cmd_node_list]
          %{context: context, root_cmd_node_list: root_cmd_node_list}

        _ ->
          "UNKNOWN"
      end
    end)
  end

  @spec gen_sql_for_get(map(), keyword(), keyword()) :: map()
  def gen_sql_for_get(context, query_object, input_kwl_node) do
    results = %{
      context: context,
      input_node_list: []
    }

    Enum.reduce(input_kwl_node, results, fn {node_name_key, fixtures_kwl_node}, results_acc ->
      %{context: context, fixture_list: fixture_list} =
        gen_sql_for_get_fixture(results_acc.context, query_object, fixtures_kwl_node)

      input_node_list = [{node_name_key, fixture_list} | results_acc.input_node_list]
      %{context: context, input_node_list: input_node_list}
    end)
  end

  @spec gen_sql_for_get_fixture(map(), keyword(), keyword()) :: map()
  def gen_sql_for_get_fixture(context, _query_object, fixtures_kwl_node) do
    result = %{
      context: context,
      fixture_list: []
    }

    Enum.reduce(fixtures_kwl_node, result, fn {node_name_key, query_config}, result_acc ->
      str_node_name_key = Atom.to_string(node_name_key)
      is_list = is_word_plural?(query_config, str_node_name_key)
      # check if the table exists in the schema
      if str_node_name_key == "" do
        IO.inspect("obugi")
        IO.inspect(query_config)
      end
      context = gen_sql_ensure_table_exists(result_acc.context, str_node_name_key, query_config)
      sql = "SELECT *"
      sql = sql <> " FROM #{sql_table_name(result_acc.context, str_node_name_key)}"
      sql = sql <> " WHERE is_deleted == 0"

      fixture_config = %{
        sql: sql,
        is_list: is_list
      }

      %{context: context, fixture_list: [{node_name_key, fixture_config}]}
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

  @spec gen_sql_ensure_table_exists(map(), binary(), map()) :: map()
  def gen_sql_ensure_table_exists(%{auto_alter_db: false} = context, _table_name, _query_config),
    do: context

  def gen_sql_ensure_table_exists(context, table_name, query_config) do
    plural_table_name = Inflex.pluralize(table_name)
    key = String.to_atom(plural_table_name)


    if context.schema |> Map.has_key?(key) == false do
      set_default_standard_pk = if Map.has_key?(query_config, :use_default_pk), do: query_config.use_default_pk, else: true
      default_standard_col_def_pk =
        if set_default_standard_pk == true do
          definition = Column.define_column(context, :id, :pk)
          definition.sql <> ","
        else
          %{
            sql: "",
            config: config
          }
        end

      columns_sql =
        if Map.has_key?(query_config, :columns) == true do
          acc= %{
            sql: "",
            table_schema: @default_table_schema
          }
          sql =
          Enum.reduce(query_config.columns, acc, fn {column_name_key, column_config}, %{ sql: sql_acc, table_schema: table_schema } ->
            col_def = Column.define_column(context, column_name_key, column_config)
            # update the schema
            schema = Map.put(table_schema, column_name_key, col_def.config)
            comma = if sql_acc == "", do: "", else: ",\n "
            sql = sql_acc <> comma <> schema.sql
            %{ sql: sql, table_schema: schema}
          end)
          sql
        else
          %{ sql: "", table_schema:  @default_table_schema}
        end

      sql = "CREATE TABLE #{sql_table_name(context, plural_table_name)} ("

          #{default_standard_pk}
          #{columns_sql},
          #{Column.define_column(context, :created_at, :datetime)},
          #{Column.define_column(context, :last_update_on, %{type: :datetime, default: "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"})},
          #{Column.define_column(context, :is_deleted, :boolean)}
          #{Column.define_column(context, :deleted_on, %{type: :datetime, default: "NULL", required: false})}}
        )
      "

      # we append at the end of the list
      auto_schema_changes = [sql | context.auto_schema_changes]
      context = %{context | auto_schema_changes: auto_schema_changes}
      # update the schema
      schema = Map.put(context.schema, key, @default_table_schema)
      %{context | schema: schema}
    else
      context
    end
  end

  def sql_table_name(context, table_name) do
    plural_table_name = Inflex.pluralize(table_name)
    "`#{context.database_name}.#{plural_table_name}`"
  end

  def try() do
    context = %{
      database_type: "mysql",
      database_name: "grocerify",
      schema: %{},
      auto_schema_changes: [],
      auto_alter_db: true
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
