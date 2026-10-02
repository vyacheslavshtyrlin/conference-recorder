package signaling

import (
	"encoding/json"
	"testing"
)

func TestRTPCodecParametersDecodeNumericMediasoupValues(t *testing.T) {
	var parameters RTPCodecParameters
	err := json.Unmarshal([]byte(`{
        "mimeType":"video/H264",
        "payloadType":102,
        "clockRate":90000,
        "parameters":{"packetization-mode":1,"level-asymmetry-allowed":1,"profile-level-id":"42e01f"}
    }`), &parameters)
	if err != nil {
		t.Fatal(err)
	}
	if parameters.Parameters["packetization-mode"] != float64(1) {
		t.Fatalf("numeric codec parameter was not preserved: %#v", parameters.Parameters)
	}
}

func TestValidateCodecMatrix(t *testing.T) {
	accepted := []struct {
		kind     MediaKind
		source   Source
		mimeType string
	}{
		{kind: KindAudio, source: SourceMic, mimeType: "audio/opus"},
		{kind: KindVideo, source: SourceCamera, mimeType: "video/VP8"},
		{kind: KindVideo, source: SourceCamera, mimeType: "video/H264"},
		{kind: KindVideo, source: SourceScreen, mimeType: "video/VP8"},
		{kind: KindVideo, source: SourceScreen, mimeType: "video/H264"},
	}
	for _, input := range accepted {
		if err := ValidateCodec(input.kind, input.source, input.mimeType); err != nil {
			t.Fatalf("expected accepted tuple %+v: %v", input, err)
		}
	}
	if err := ValidateCodec(KindVideo, SourceCamera, "video/AV1"); err == nil {
		t.Fatal("unsupported AV1 codec was accepted")
	}
	if err := ValidateCodec(KindVideo, SourceMic, "video/VP8"); err == nil {
		t.Fatal("invalid kind/source pair was accepted")
	}
}

func TestRouterSupportsCodecReportsCurrentH264Gap(t *testing.T) {
	capabilities := json.RawMessage(`{
        "codecs": [
            {"mimeType":"audio/opus"},
            {"mimeType":"video/VP8"}
        ]
    }`)
	supported, err := RouterSupportsCodec(capabilities, "video/H264")
	if err != nil {
		t.Fatal(err)
	}
	if supported {
		t.Fatal("H264 was reported for the current Opus/VP8 router shape")
	}
}
