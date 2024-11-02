package spendreport.detailed.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Represents a detailed alert with additional information, such as the zip code
 * associated with the potential fraud. This class is used to store and transfer
 * alert data in the Flink pipeline.
 *
 * Annotations from Lombok are used to automatically generate boilerplate code
 * like constructors, getters, setters, equals, hashCode, and toString methods.
 */
@NoArgsConstructor // Generates a no-argument constructor
@AllArgsConstructor // Generates an all-argument constructor
@Getter // Generates getters for all fields
@Setter // Generates setters for all fields
@EqualsAndHashCode // Generates equals() and hashCode() methods
@ToString // Generates a toString() method
public final class DetailedAlert {

    // ID of the account associated with the alert
    private long id;

    // Message associated with the alert, typically containing details of the fraud
    private String message;
}
