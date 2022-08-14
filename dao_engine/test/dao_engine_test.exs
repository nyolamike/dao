defmodule DaoEngineTest do
  use ExUnit.Case
  doctest DaoEngine

  test "greets the world" do
    assert DaoEngine.hello() == :world
  end
end
