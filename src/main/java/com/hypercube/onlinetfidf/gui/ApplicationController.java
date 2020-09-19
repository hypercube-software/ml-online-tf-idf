package com.hypercube.onlinetfidf.gui;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hypercube.onlinetfidf.model.Document;
import com.hypercube.onlinetfidf.model.Response;
import com.hypercube.onlinetfidf.tfidf.StateManager;

@Controller
public class ApplicationController {
	@Autowired
	StateManager state;
	
	@RequestMapping(value="/pushDocument", method = RequestMethod.PUT)
	public @ResponseBody Response pushDocument(@RequestBody Document document) {
		
		state.updateState(document);
		
		Response r = new Response();
		r.setMessage("OK");
		return r;
	}
}
