defmodule DaoEngineTest do
  use ExUnit.Case
  doctest DaoEngine

  alias DaoEngine, as: Dao

  test "indicates a word is plural or false" do
    query_config = %{}
    assert true == Dao.is_word_plural?(query_config, "shops")
    assert false == Dao.is_word_plural?(query_config, "shop")
    # by default
    assert true == Dao.is_word_plural?(query_config, "aircraft")
    assert true == Dao.is_word_plural?(query_config, "deer")
    # forcing singluar on umbigious situations
    assert false == Dao.is_word_plural?(%{is_list: false}, "aircraft")
    assert false == Dao.is_word_plural?(%{is_list: false}, "deer")
  end
end
