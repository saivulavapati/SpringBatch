package com.practice.config;

import com.practice.entity.Person;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class PersonFieldSetMapper implements FieldSetMapper<Person> {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("M/d/yyyy HH:mm");

    @Override
    public Person mapFieldSet(FieldSet fieldSet) throws BindException {
        Person person = new Person();
        person.setId(Integer.parseInt(fieldSet.readString("id")));
        person.setFirstName(fieldSet.readString("firstName"));
        person.setLastName(fieldSet.readString("lastName"));
        person.setEmail(fieldSet.readString("email"));
        person.setUserStatus(fieldSet.readString("user_status"));

        String dateString = fieldSet.readString("create_ts");
        if (dateString != null && !dateString.isBlank()) {
            person.setCreateTs(LocalDateTime.parse(dateString, FORMATTER));;
        }

        return person;
    }
}
