defmodule Utils do
  def ensure_key(map, key, default_value) do
    if Map.has_key?(map, key) == true do
      map
    else
      Map.put(map, key, default_value)
    end
  end

  def ensure_key(map, key, replace_key, default_value) do
    if Map.has_key?(map, key) == true do
      map |> Map.put(replace_key, map[key]) |> Map.delete(key)
    else
      map |> Map.put(replace_key, default_value) |> Map.delete(key)
    end
  end

  def ensure_key(map, key, replace_key, default_value, map_to_modify) do
    if Map.has_key?(map, key) == true do
      {map_to_modify |> Map.put(replace_key, map[key]), map |> Map.delete(key)}
    else
      if Map.has_key?(map, replace_key) == true do
        {map_to_modify |> Map.put(replace_key, map[replace_key]), map |> Map.delete(key)}
      else
        {map_to_modify |> Map.put(replace_key, default_value), map |> Map.delete(key)}
      end
    end
  end

  def to_plural(word) do
    Inflex.pluralize(word)
  end

  def is_word_plural?(word) do
    plural_word = Inflex.pluralize(word)
    plural_word == word
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

  def log(label, payLoad) do
    IO.inspect("-----> Start #{label}: ----------------------------------")
    IO.inspect(payLoad)
    IO.inspect("-----> End #{label}: <-----------------------------------")
  end
end
