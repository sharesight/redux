defmodule Redux do
  @moduledoc """
  Provides a wrapper around Redis - methods follow the Redis command and take
  paraneters as Elixir maps, etc
  """

  # Maximum arguments we send to Redis in one chunk
  @chunk_size 256

  @doc """
  Delete a top level Redis key

  ## Params

   - top_level_key: the key string or atm
  """
  def del(top_level_key) do
    Redix.command(:redix, ["DEL", "#{top_level_key}"])
  end

  @doc """
  Set a value in Redis

  ## Params

    - key: the key string or atom
    - value: the value, which will be encoded in JSON if not scalar
  """
  def set(key, value) do
    Redix.command(:redix, ["SET", "#{key}", encode_if_needed(value)])
  end

  @doc """
  Get a value from Redis, decoding if needed

  ## Params

    - key: the key string or atom
  """
  def get(key) do
    {status, value_str} = Redix.command(:redix, ["GET", "#{key}"])

    cond do
      status != :ok -> nil
      is_nil(value_str) -> nil
      String.starts_with?(value_str, "{") -> Poison.decode!(value_str)
      true -> value_str
    end
  end

  @doc """
  Set multiple values in a Redis hash

  ## Params

    - hash_name: the hash name as a string or atom
    - map_to_write: a map of key => value, values will be Json encoded if not scalar
  """
  def hmset(hash_name, map_to_write) do
    Enum.chunk_every(Map.to_list(map_to_write), @chunk_size)
    |> Enum.each(fn c ->
      fields =
        Enum.map(c, fn {k, v} -> ["#{k}", encode_if_needed(v)] end)
        |> Enum.concat()

      Redix.command(:redix, Enum.concat(["HMSET", hash_name], fields))
    end)
  end

  @doc """
  Set a single value in a Redis hash

  ## Params

    - hash_name: the hash name as a string or atom
    - key: the key as a string or atom
    - value: the value, which will be encoded in JSON if not scalar
  """
  def hset(hash_name, key, value) do
    Redix.command(:redix, ["HSET", hash_name, "#{key}", encode_if_needed(value)])
  end

  @doc """
  Delete keys from a Redis hash

  ## Params

    - hash_name: the hash name as a string or atom
    - keys: A list of keys or a single key as a string or atom
  """
  def hdel(hash_name, keys) do
    keys_str =
      if Enumerable.impl_for(keys) do
        Enum.join(keys, " ")
      else
        keys
      end

    # Logger.debug "HDEL #{keys_str}"
    Redix.command(:redix, ["HDEL", hash_name, "#{keys_str}"])
  end

  @doc """
  Return a list of keys in a Redis hash

  ## Params

    - hash_name: the hash name as a string or atom
    - key: the key as a string or atom
  """
  def hkeys(hash_name) do
    {_, keys} = Redix.command(:redix, ["HKEYS", "#{hash_name}"])
    keys
  end

  def hscan(hash_name, pattern, cursor \\ "0") do
    # Logger.debug "HSCAN #{pattern} #{cursor}"
    {_, [new_cursor, result_keys_values]} =
      Redix.command(:redix, ["HSCAN", hash_name, cursor, "MATCH", pattern])

    result_map =
      Enum.chunk_every(result_keys_values, 2) |> Enum.map(fn [a, b] -> {a, b} end) |> Map.new()

    if new_cursor == "0", do: %{}, else: hscan(hash_name, pattern, new_cursor) |> Map.merge(result_map)
  end

  def hscan_to(callback, hash_name, pattern, cursor \\ "0") do
    {_, [new_cursor, result_keys_values]} =
      Redix.command(:redix, ["HSCAN", hash_name, cursor, "MATCH", pattern])

    Enum.chunk_every(result_keys_values, 2)
    |> Enum.map(fn [a, b] -> {a, b} end)
    |> Map.new()
    |> callback.()

    if new_cursor == "0", do: %{}, else: hscan_to(callback, hash_name, pattern, new_cursor)
  end

  @doc """
  Return a keyed value in a Redis hash, decoding JSON if needed.
  (based on the value starting with a '{')

  ## Params

    - hash_name: the hash name as a string or atom

  """
  def hget(hash_name, key) do
    {_, value_str} = Redix.command(:redix, ["HGET", "#{hash_name}", "#{key}"])

    cond do
      is_nil(value_str) -> nil
      String.starts_with?(value_str, "{") -> Poison.decode!(value_str)
      true -> value_str
    end
  end

  # We pass strings and numbers as literals, otherwise encode into JSON
  defp encode_if_needed(s) when is_binary(s) do
    s
  end

  defp encode_if_needed(n) when is_number(n) do
    "#{n}"
  end

  defp encode_if_needed(v) do
    Poison.encode!(v)
  end
end
