defmodule ReduxTest do
  use ExUnit.Case
  doctest Redux

  test "greets the world" do
    assert Redux.hello() == :world
  end
end
