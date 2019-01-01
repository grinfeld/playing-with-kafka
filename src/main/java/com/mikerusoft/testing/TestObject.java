package com.mikerusoft.testing;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.io.Serializable;
import java.util.Date;
import java.util.Optional;

@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true, builderClassName = "Builder")
public class TestObject implements Serializable {
    @Getter @Setter private String value;
    @Getter @Setter private int number;

    @JsonIgnore private Date date;

    @JsonProperty
    public Long getDate() {
        return Optional.ofNullable(date).map(Date::getTime).orElse(null);
    }

    @JsonProperty
    public void setDate(Long date) {
        this.date = Optional.ofNullable(date).map(Date::new).orElse(null);
    }
}
