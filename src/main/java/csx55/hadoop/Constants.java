package csx55.hadoop;

public class Constants {
    // Constants related to song metadata
    public static class Analysis {
        public static final int SONG_ID_INDEX = 0;
        public static final int SONG_HOTTNESSS_INDEX = 1;
        public static final int ANALYSIS_SAMPLE_RATE_INDEX = 2;
        public static final int DANCEABILITY_INDEX = 3;
        public static final int DURATION_INDEX = 4;
        public static final int END_OF_FADE_IN_INDEX = 5;
        public static final int ENERGY_INDEX = 6;
        public static final int KEY_INDEX = 7;
        public static final int KEY_CONFIDENCE_INDEX = 8;
        public static final int LOUDNESS_INDEX = 9;
        public static final int MODE_INDEX = 10;
        public static final int MODE_CONFIDENCE_INDEX = 11;
        public static final int START_OF_FADE_OUT_INDEX = 12;
        public static final int TEMPO_INDEX = 13;
        public static final int TIME_SIGNATURE_INDEX = 14;
        public static final int TIME_SIGNATURE_CONFIDENCE_INDEX = 15;
        public static final int TRACK_ID_INDEX = 16;
        public static final int SEGMENTS_START_INDEX = 17;
        public static final int SEGMENTS_CONFIDENCE_INDEX = 18;
        public static final int SEGMENTS_PITCHES_INDEX = 19;
        public static final int SEGMENTS_TIMBRE_INDEX = 20;
        public static final int SEGMENTS_LOUDNESS_MAX_INDEX = 21;
        public static final int SEGMENTS_LOUDNESS_MAX_TIME_INDEX = 22;
        public static final int SEGMENTS_LOUDNESS_START_INDEX = 23;
        public static final int SECTIONS_START_INDEX = 24;
        public static final int SECTIONS_CONFIDENCE_INDEX = 25;
        public static final int BEATS_START_INDEX = 26;
        public static final int BEATS_CONFIDENCE_INDEX = 27;
        public static final int BARS_START_INDEX = 28;
        public static final int BARS_CONFIDENCE_INDEX = 29;
        public static final int TATUMS_START_INDEX = 30;
        public static final int TATUMS_CONFIDENCE_INDEX = 31;
    }

    // Constants related to artist metadata
    public static class Metadata {
        public static final int ARTIST_FAMILIARITY_INDEX = 0;
        public static final int ARTIST_HOTTNESSS_INDEX = 1;
        public static final int ARTIST_ID_INDEX = 2;
        public static final int ARTIST_LATITUDE_INDEX = 3;
        public static final int ARTIST_LONGITUDE_INDEX = 4;
        public static final int ARTIST_LOCATION_INDEX = 5;
        public static final int ARTIST_NAME_INDEX = 6;
        public static final int SONG_ID_INDEX = 7;
        public static final int TITLE_INDEX = 8;
        public static final int SIMILAR_ARTISTS_INDEX = 9;
        public static final int ARTIST_TERMS_INDEX = 10;
        public static final int ARTIST_TERMS_FREQ_INDEX = 11;
        public static final int ARTIST_TERMS_WEIGHT_INDEX = 12;
        public static final int YEAR_INDEX = 13;
    }
}
