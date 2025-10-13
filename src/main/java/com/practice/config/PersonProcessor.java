package com.practice.config;

import org.springframework.batch.item.ItemProcessor;

import com.practice.entity.Person;

public class PersonProcessor implements ItemProcessor<Person, Person> {

	@Override
	public Person process(Person person) throws Exception {
//		if(person.getFirstName().equalsIgnoreCase("david")) {
//			return person;
//		}
		return person;
	}

}
