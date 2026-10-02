package recording

import "testing"

func TestCanonicalIdentityIsStableAcrossReconnectAndProducerReplacement(t *testing.T) {
	recordingID := "rec_01H"
	participantID := ParticipantInstanceID(recordingID, "peer_alice")
	reconnectedID := ParticipantInstanceID(recordingID, "peer_alice")
	newParticipantID := ParticipantInstanceID(recordingID, "peer_alice_new_session")

	if participantID != reconnectedID {
		t.Fatal("same v1 participant ID did not retain its opaque instance")
	}
	if participantID == newParticipantID {
		t.Fatal("new v1 participant ID reused an opaque instance")
	}
	if len(participantID) != len("pi_")+43 {
		t.Fatalf("unexpected participant digest length: %q", participantID)
	}
	const goldenParticipant = "pi_cZDv-6zLCSo_UzMztKuXIYYCaQBj0cwnCQuMhkEIA7s"
	if participantID != goldenParticipant {
		t.Fatalf("participant golden vector changed: %q", participantID)
	}

	beforeReplacement, err := TrackID(recordingID, participantID, SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	afterReplacement, err := TrackID(recordingID, participantID, SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	screen, err := TrackID(recordingID, participantID, SourceScreen)
	if err != nil {
		t.Fatal(err)
	}
	if beforeReplacement != afterReplacement {
		t.Fatal("producer replacement changed logical camera track")
	}
	if beforeReplacement == screen {
		t.Fatal("camera and screen mapped to the same logical track")
	}
	const goldenCamera = "trk_KbvhWZMJr-aJBVFNZwG3k4_CgRxxgMUQzZIXm4o_uFQ"
	const goldenScreen = "trk_pQM2nHh6-GdjqGXhXm47w2t_u6aD4Esl-zKn6xVVRIU"
	if beforeReplacement != goldenCamera || screen != goldenScreen {
		t.Fatalf("track golden vectors changed: camera=%q screen=%q", beforeReplacement, screen)
	}
}

func TestTrackIDRejectsUnsupportedSource(t *testing.T) {
	_, err := TrackID("rec", ParticipantInstanceID("rec", "peer"), Source("system"))
	if err == nil {
		t.Fatal("unsupported source was accepted")
	}
}
