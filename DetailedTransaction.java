package spendreport.detailed.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Represents a transaction with additional details, including the zip code.
 * This class is used to model incoming transactions in the Flink pipeline.
 *
 * The zip code is uniformly selected from a predefined set of values:
 * [01003, 02115, 78712].
 *
 * Lombok annotations are used to automatically generate boilerplate code
 * such as constructors, getters, setters, and toString methods.
 */
@NoArgsConstructor // Generates a no-argument constructor
@AllArgsConstructor // Generates a constructor with all fields as arguments
@Getter // Generates getters for all fields
@Setter // Generates setters for all fields
@ToString // Generates a toString() method for debugging
public class DetailedTransaction {

    // Serialization ID to maintain compatibility during serialization
    private static final long serialVersionUID = 1L;

    // ID of the account associated with the transaction
    private long accountId;

    // Timestamp of when the transaction occurred
    private long timestamp;

    // Amount of the transaction
    private double amount;

    // Zip code associated with the transaction
    private String zipCode;
}
