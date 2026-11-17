defmodule Nostrum.Voice.AudioTest do
  use ExUnit.Case, async: true

  alias Nostrum.Struct.VoiceState
  alias Nostrum.Voice.Audio

  @guild_id 1
  @frame_usec 20_000
  @frame_samples 960

  # Stands in for `Nostrum.Voice`: keeps the voice state the player updates.
  defmodule Voices do
    use GenServer

    def start_link(voice), do: GenServer.start_link(__MODULE__, voice)

    @impl true
    def init(voice), do: {:ok, %{voice | voice_pid: self()}}

    @impl true
    def handle_call({:update, _guild_id, args}, _from, voice) do
      voice = Map.merge(voice, Map.new(args))
      {:reply, voice, voice}
    end

    def handle_call({:get, _guild_id}, _from, voice), do: {:reply, voice, voice}
  end

  describe "sync_clock/2" do
    test "the first frame is due now" do
      voice = Audio.sync_clock(%VoiceState{rtp_timestamp: 4_800}, 1_000_000)

      assert voice.rtp_timestamp == 4_800
      assert Audio.due_at(voice) == 1_000_000
    end

    test "a frame that is a little late keeps its timestamp" do
      voice = Audio.sync_clock(%VoiceState{rtp_timestamp: 0}, 0)

      assert Audio.sync_clock(voice, 3 * @frame_usec - 1) == voice
    end

    test "the timestamp skips the frames that were due while nothing was sent" do
      voice = Audio.sync_clock(%VoiceState{rtp_timestamp: 0}, 0)

      voice = Audio.sync_clock(voice, 1_750_000)

      assert voice.rtp_timestamp == 87 * @frame_samples
      # 1.75s is 87.5 frames: the half frame is caught up on, not skipped.
      assert Audio.due_at(voice) == 1_740_000
    end

    test "a frame that is not due yet keeps its timestamp" do
      voice = Audio.sync_clock(%VoiceState{rtp_timestamp: 9_600}, 0)

      assert Audio.sync_clock(voice, -100_000) == voice
    end
  end

  describe "the player" do
    setup do
      {:ok, receiver} = :gen_udp.open(0, [:binary, active: true, ip: {127, 0, 0, 1}])
      {:ok, port} = :inet.port(receiver)

      voices =
        start_supervised!(
          {Voices,
           %VoiceState{
             guild_id: @guild_id,
             session_pid: self(),
             ip: "127.0.0.1",
             port: port,
             ssrc: 7,
             secret_key: :crypto.strong_rand_bytes(32),
             udp_socket: Audio.open_udp(),
             encryption_mode: :xsalsa20_poly1305,
             rtp_sequence: 0,
             rtp_timestamp: 0
           }}
        )

      %{voices: voices}
    end

    test "sends the first frame at once and each frame when it is due", %{voices: voices} do
      started = now()
      play(voices, frames(10), 1)
      packets = packets(15)

      # 10 frames of audio and 5 of silence
      assert Enum.map(packets, & &1.sequence) == Enum.to_list(0..14)
      assert Enum.map(packets, & &1.timestamp) == Enum.map(0..14, &(&1 * @frame_samples))

      [first | _] = packets
      assert first.at - started < 100_000

      # Frame n is never sent early, and the last one is not late by much.
      for {packet, n} <- Enum.with_index(packets) do
        assert packet.at - first.at >= (n - 1) * @frame_usec
      end

      assert List.last(packets).at - first.at < 14 * @frame_usec + 150_000
    end

    test "sends a burst when its first frame is due", %{voices: voices} do
      play(voices, frames(8), 4)
      [first | _] = packets = packets(13)

      assert Enum.map(packets, & &1.timestamp) == Enum.map(0..12, &(&1 * @frame_samples))
      # The second burst (frames 4..7) and the silence after it wait their turn.
      assert Enum.at(packets, 4).at - first.at >= 3 * @frame_usec
      assert Enum.at(packets, 8).at - first.at >= 7 * @frame_usec
    end

    test "moves the timestamp on by the pause between two sounds", %{voices: voices} do
      play(voices, frames(2), 1)
      first = packets(7)

      Process.sleep(300)

      play(voices, frames(2), 1)
      second = packets(7)

      last = List.last(first)
      [next | _] = second

      # Sequence numbers go on, timestamps skip what the pause was long.
      assert next.sequence == last.sequence + 1

      paused_frames = div(next.at - last.at, @frame_usec)
      skipped_frames = div(next.timestamp - last.timestamp, @frame_samples)

      assert paused_frames >= 15
      assert_in_delta skipped_frames, paused_frames, 3
    end

    test "a source that stalls moves the timestamp on too", %{voices: voices} do
      source =
        Stream.unfold(0, fn
          3 -> nil
          1 -> Process.sleep(200) == :ok && {<<1>>, 2}
          n -> {<<n>>, n + 1}
        end)

      play(voices, source, 1, true)
      [first, second | _] = packets(8)

      assert div(second.timestamp - first.timestamp, @frame_samples) in 8..12
    end
  end

  defp frames(count), do: for(n <- 1..count, do: <<n>>)

  defp play(voices, source, frames_per_burst, stateful \\ false) do
    source = if stateful, do: stateful(source), else: source

    voice =
      GenServer.call(voices, {:update, @guild_id, raw_audio: source, raw_stateful: stateful})

    spawn_link(Audio, :start_player, [voice, %{audio_frames_per_burst: frames_per_burst}])
  end

  # An enumerable that goes on where the last enumeration stopped.
  defp stateful(source) do
    {:ok, agent} = Agent.start_link(fn -> source end)

    Stream.unfold(agent, fn agent ->
      Agent.get_and_update(
        agent,
        fn rest ->
          case Enum.take(rest, 1) do
            [frame] -> {{frame, agent}, Stream.drop(rest, 1)}
            [] -> {nil, []}
          end
        end,
        :infinity
      )
    end)
  end

  # The packets of one playback, which is over when the player stops speaking.
  # The packets come through the socket and may arrive after that message.
  defp packets(count) do
    packets =
      for _ <- 1..count do
        receive do
          {:udp, _socket, _ip, _port,
           <<0x80, 0x78, sequence::16, timestamp::32, 7::32, _::binary>>} ->
            %{sequence: sequence, timestamp: timestamp, at: now()}
        after
          5_000 -> flunk("the player sent fewer than #{count} packets")
        end
      end

    assert_receive {:"$gen_cast", {:speaking, false, _timed_out}}, 5_000
    refute_receive {:udp, _socket, _ip, _port, _packet}, 50
    packets
  end

  defp now, do: System.monotonic_time(:microsecond)
end
