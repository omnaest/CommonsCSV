/*

	Copyright 2017 Danny Kunz

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.


*/
package org.omnaest.utils.csv;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

public class CSVUtilsTest
{

	@Test
	public void testParseInputStream() throws Exception
	{
		List<Map<String, String>> data = CSVUtils	.parse(this	.getClass()
																.getResourceAsStream("/ACTIVITIES.csv"))
													.collect(Collectors.toList());

		//System.out.println(JSONHelper.prettyPrint(data.get(0)));

		/*
		 * {
		 * "ACTIVITY" : "5-HT-Inhibitor",
		 * "ACTUPPER" : "5-HT-INHIBITOR",
		 * "DEFINITION" : "5-HT-Inhibitor",
		 * "REFERENCE" : "",
		 * "USERID" : "DUKE",
		 * "CREATED" : "10-Dec-97",
		 * "MODIFIED" : ""
		 * }
		 */
		this.assertContentValidity(data);
	}

	@Test
	public void testSerialize() throws IOException
	{
		Stream<Map<String, String>> dataStream = CSVUtils.parse(this.getClass()
																	.getResourceAsStream("/ACTIVITIES.csv"));
		String serialization = CSVUtils	.serialize(dataStream)
										.get();
		List<Map<String, String>> data = CSVUtils	.parse(serialization)
													.collect(Collectors.toList());
		this.assertContentValidity(data);
	}

	private void assertContentValidity(List<Map<String, String>> data)
	{
		assertEquals(2432, data.size());
		assertEquals(	Arrays	.asList("ACTIVITY", "ACTUPPER", "DEFINITION", "REFERENCE", "USERID", "CREATED", "MODIFIED")
								.stream()
								.collect(Collectors.toSet()),
						data.get(0)
							.keySet());
		assertEquals("5-HT-Inhibitor", data	.get(0)
											.get("DEFINITION"));
	}

}
