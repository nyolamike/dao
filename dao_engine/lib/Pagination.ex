defmodule Pagination do
  def preprocess_config(context, preprocess_config_table_def, config_table_def) do
    preprocess_config_table_def =
      cond do
        Map.has_key?(config_table_def, "dao@limit") ->
          Map.put(preprocess_config_table_def, "limit", config_table_def["dao@limit"])

        Map.has_key?(config_table_def, "dao@size") ->
          Map.put(
            preprocess_config_table_def,
            "limit",
            config_table_def["dao@size"]
          )

        Map.has_key?(config_table_def, "dao@page_size") ->
          Map.put(
            preprocess_config_table_def,
            "limit",
            config_table_def["dao@page_size"]
          )

        Map.has_key?(config_table_def, "dao@pagesize") ->
          Map.put(
            preprocess_config_table_def,
            "limit",
            config_table_def["dao@pagesize"]
          )

        Map.has_key?(config_table_def, "dao@take") ->
          Map.put(
            preprocess_config_table_def,
            "limit",
            config_table_def["dao@take"]
          )

        true ->
          preprocess_config_table_def
      end

    {context, preprocess_config_table_def}
  end

  def gen_sql(context, query_config) do
    pagination_sql =
      if is_map(query_config) do
        processed_query_config = Table.preprocess_query_config(context, query_config)

        cond do
          Map.has_key?(processed_query_config, "limit") &&
              is_binary(processed_query_config["limit"]) ->
            # one has specified using string so we trim and convert to integer
            # nyd: validate that its just a number specified here
            processed_query_config["limit"]
            |> String.trim()

          Map.has_key?(processed_query_config, "limit") &&
              is_integer(processed_query_config["limit"]) ->
            Integer.to_string(processed_query_config["limit"])

          true ->
            # nyd: generaate error that limit expects a str number or an integer
            ""
        end
      else
        ""
      end

    if pagination_sql == "", do: "", else: " LIMIT #{pagination_sql}"
  end
end
