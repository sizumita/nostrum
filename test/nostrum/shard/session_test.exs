defmodule Nostrum.Shard.SessionTest do
  use ExUnit.Case, async: true

  alias Nostrum.Shard.Session
  alias Nostrum.Struct.WSState

  @moduletag capture_log: true

  # A stand-in for a `:gun` connection pid. The session only uses it as an
  # identity and to send it messages, so any live process will do.
  defp fake_conn do
    pid = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp ws_state(attrs) do
    struct!(
      %WSState{
        conn_pid: self(),
        shard_num: 0,
        total_shards: 1,
        gateway: "gateway.discord.gg",
        resume_gateway: "wss://gateway-us-east1-b.discord.gg",
        session: "session-id",
        seq: 42,
        bot_options: %{name: :test_bot},
        heartbeat_interval: 41_250
      },
      attrs
    )
  end

  describe "messages from a previous connection" do
    setup do
      %{old_conn: fake_conn(), data: ws_state(conn: fake_conn(), stream: make_ref())}
    end

    for state <- [:disconnected, :connecting_http, :connecting_ws, :connected] do
      test "are ignored in #{state}", %{old_conn: old_conn, data: data} do
        stream = make_ref()

        for message <- [
              {:gun_down, old_conn, :ws, :closed, []},
              {:gun_ws, old_conn, stream, :close},
              {:gun_ws, old_conn, stream, {:close, 1000, ""}},
              {:gun_ws, old_conn, stream, {:binary, <<1, 2, 3>>}},
              {:gun_error, old_conn, stream, :timeout},
              {:gun_upgrade, old_conn, stream, ["websocket"], []}
            ] do
          assert :keep_state_and_data = apply(Session, unquote(state), [:info, message, data])
        end
      end
    end
  end

  describe "connecting_ws/3 when the connection fails during the upgrade" do
    setup do
      %{data: ws_state(conn: fake_conn(), stream: make_ref())}
    end

    test "connects again later on gun_down and keeps the session for resuming", %{data: data} do
      message = {:gun_down, data.conn, :http, :closed, [data.stream]}

      assert {:next_state, :disconnected, new_data, {:state_timeout, delay, :connect}} =
               Session.connecting_ws(:info, message, data)

      assert delay > 0
      assert %{conn: nil, stream: nil, seq: 42, session: "session-id"} = new_data
    end

    test "connects again after retry-after when the gateway rejects the upgrade", %{data: data} do
      # What Cloudflare sent in front of the gateway when this crashed in production.
      headers = [
        {"content-length", "19"},
        {"connection", "keep-alive"},
        {"retry-after", "5"},
        {"server", "cloudflare"}
      ]

      message = {:gun_response, data.conn, data.stream, :nofin, 503, headers}

      assert {:next_state, :disconnected, %{conn: nil, seq: 42, session: "session-id"},
              {:state_timeout, 5_000, :connect}} =
               Session.connecting_ws(:info, message, data)
    end

    test "uses the default delay when retry-after is missing or not seconds", %{data: data} do
      for headers <- [[], [{"retry-after", "Fri, 13 Nov 2026 18:02:12 GMT"}]] do
        message = {:gun_response, data.conn, data.stream, :nofin, 503, headers}

        assert {:next_state, :disconnected, _, {:state_timeout, 1_000, :connect}} =
                 Session.connecting_ws(:info, message, data)
      end
    end

    test "does not wait longer than 30 seconds for retry-after", %{data: data} do
      message = {:gun_response, data.conn, data.stream, :nofin, 503, [{"retry-after", "3600"}]}

      assert {:next_state, :disconnected, _, {:state_timeout, 30_000, :connect}} =
               Session.connecting_ws(:info, message, data)
    end

    test "connects again later on gun_error", %{data: data} do
      for message <- [
            {:gun_error, data.conn, data.stream, :closed},
            {:gun_error, data.conn, :closed}
          ] do
        assert {:next_state, :disconnected, %{conn: nil}, {:state_timeout, _, :connect}} =
                 Session.connecting_ws(:info, message, data)
      end
    end

    test "drops messages the failed connection already sent", %{data: data} do
      send(self(), {:gun_data, data.conn, data.stream, :fin, "Service Unavailable"})
      message = {:gun_response, data.conn, data.stream, :nofin, 503, []}

      assert {:next_state, :disconnected, _, _} = Session.connecting_ws(:info, message, data)

      refute_received {:gun_data, _, _, _, _}
    end
  end

  describe "connecting_http/3 when the connection fails" do
    test "connects again later on gun_down" do
      data = ws_state(conn: fake_conn())
      message = {:gun_down, data.conn, :http, :closed, []}

      assert {:next_state, :disconnected, %{conn: nil}, {:state_timeout, _, :connect}} =
               Session.connecting_http(:info, message, data)
    end
  end

  describe "disconnected/3" do
    test "connects when the retry delay has passed" do
      data = ws_state(conn: nil)

      assert {:next_state, :connecting_http, ^data} =
               Session.disconnected(:state_timeout, :connect, data)
    end

    test "postpones user requests until connected" do
      assert {:keep_state_and_data, :postpone} =
               Session.disconnected(:cast, {:update_voice_state, <<>>}, ws_state(conn: nil))
    end
  end

  describe "disconnect requests" do
    for state <- [:disconnected, :connecting_http, :connecting_ws, :connected] do
      test "are answered with the resume information in #{state}" do
        data = ws_state(conn: fake_conn(), stream: make_ref())
        from = {self(), make_ref()}

        assert {:stop_and_reply, :normal, {:reply, ^from, reply}} =
                 apply(Session, unquote(state), [{:call, from}, {:disconnect, nil}, data])

        assert %{shard_num: 0, seq: 42, session: "session-id", bot_options: %{name: :test_bot}} =
                 reply
      end
    end
  end

  describe "connected/3 when a heartbeat is not acknowledged" do
    test "closes the old connection before connecting again" do
      data = ws_state(conn: fake_conn(), stream: make_ref(), heartbeat_ack: false)
      send(self(), {:gun_down, data.conn, :ws, :closed, [data.stream]})

      assert {:next_state, :disconnected, %{conn: nil, stream: nil, seq: 42},
              {:next_event, :internal, :connect}} =
               Session.connected(:state_timeout, :send_heartbeat, data)

      refute_received {:gun_down, _, _, _, _}
    end
  end
end
