package mil.nga.giat.geowave.format.stanag4676.parser.model;

//STANAG 4676
/**
 * Provides the type and source of information from which information was
 * computed or derived
 */
public enum ModalityType {
	/**
	 * the information, estimate, or determination is derived from a radar
	 * Doppler source
	 */
	DOPPLER_SIGNATURE(
			"DOPPLER_SIGNATURE"),

	/**
	 * the information, estimate, or determination is derived from a radar High
	 * Range Resolution source.
	 */
	HRR_SIGNATURE(
			"HRR_SIGNATURE"),

	/**
	 * the information, estimate, or determination is derived from a Still or
	 * Video source.
	 */
	IMAGE_SIGNATURE(
			"IMAGE_SIGNATURE"),

	/**
	 * the information, estimate, or determination is derived from a Human
	 * Intelligence source.
	 */
	HUMINT(
			"HUMINT"),

	/**
	 * the information, estimate, or determination is derived from a Measurement
	 * and Signal Intelligence source.
	 */
	MASINT(
			"MASINT"),

	/**
	 * the information, estimate, or determination is derived from a Electronics
	 * Intelligence source.
	 */
	ELINT(
			"ELINT"),

	/**
	 * the information, estimate, or determination is derived from a
	 * Communications Intelligence Externals source.
	 */
	COMINT_EXTERNALS(
			"COMINT_EXTERNALS"),

	/**
	 * the information, estimate, or determination is derived from a
	 * Communications Intelligence Internals source.
	 */
	COMINT_INTERNALS(
			"COMINT_INTERNALS"),

	/**
	 * the information, estimate, or determination is derived from a Open Source
	 * Intelligence source. (publicly available)
	 */
	OSINT(
			"OSINT"),

	/**
	 * the information, estimate, or determination is derived from a Biometrics
	 * source.
	 */
	BIOMETRICS(
			"BIOMETRICS"),

	/**
	 * the information, estimate, or determination is derived from an Automated
	 * Identification System source.
	 */
	AIS(
			"AIS"),

	/**
	 * the information, estimate, or determination is derived from a Blue Force
	 * Tracking source.
	 */
	BFT(
			"BFT"),

	/**
	 * the information, estimate, or determination is derived from a combination
	 * of two or more sources.
	 */
	MIXED(
			"MIXED"),

	/**
	 * the information, estimate, or determination is derived from other types
	 * of sources, such as Link 16.
	 */
	OTHER(
			"OTHER");

	private String value;

	ModalityType() {
		this.value = ModalityType.values()[0].toString();
	}

	ModalityType(
			final String value ) {
		this.value = value;
	}

	public static ModalityType fromString(
			String value ) {
		for (final ModalityType item : ModalityType.values()) {
			if (item.toString().equals(
					value)) {
				return item;
			}
		}
		return null;
	}

	@Override
	public String toString() {
		return value;
	}
}
