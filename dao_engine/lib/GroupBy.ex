defmodule GroupBy do
  def preprocess_config(context, preprocess_config_table_def, config_table_def) do
    preprocess_config_table_def =
      cond do
        Map.has_key?(config_table_def, "dao@group_by") ->
          Map.put(preprocess_config_table_def, "group_by", config_table_def["dao@group_by"])

        true ->
          preprocess_config_table_def
      end

    {context, preprocess_config_table_def}
  end

  def gen_sql(context, query_config) do
    group_by_sql =
      if is_map(query_config) do
        processed_query_config = Table.preprocess_query_config(context, query_config)

        cond do
          Map.has_key?(processed_query_config, "group_by") &&
              is_binary(processed_query_config["group_by"]) ->
            # one has specified using string so we trim and split using spaces
            orderby_list =
              processed_query_config["group_by"]
              |> String.trim()
              |> String.split(trim: true)

            Enum.reduce(orderby_list, "", fn column, sql_acc ->
              comma = if sql_acc == "", do: "", else: ", "
              "#{sql_acc}#{comma}#{column}"
            end)
            |> String.trim()

          Map.has_key?(processed_query_config, "group_by") &&
              is_list(processed_query_config["group_by"]) ->
            Enum.reduce(processed_query_config["group_by"], "", fn column, sql_acc ->
              comma = if sql_acc == "", do: "", else: ", "
              "#{sql_acc}#{comma}#{column}"
            end)
            |> String.trim()

          true ->
            ""
        end
      else
        ""
      end

    if group_by_sql == "", do: "", else: " GROUP BY #{group_by_sql}"
  end
end
