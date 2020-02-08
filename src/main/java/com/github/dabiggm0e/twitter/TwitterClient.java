package com.github.dabiggm0e.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class TwitterClient {

    Logger logger;
    Twitter twitter;

    public TwitterClient() {
        logger = LoggerFactory.getLogger(TwitterClient.class.getName());
        twitter = TwitterFactory.getSingleton();
    }

    public Collection<String> getHomeTimeline(int pageNumber, int pageSize) throws TwitterException {
        Paging paging = new Paging(pageNumber, pageSize);

        List<Status> statuses = twitter.getHomeTimeline(paging);

        logger.info("Showing twitter home timeline. Page: " + pageNumber);
        Collection<String> statusCollection = null;

        for(Status status: statuses) {
            String statusLine = "@" + status.getUser().getScreenName() + "(" +
                            status.getUser().getName() + "): " + status.getText();
            statusCollection.add(statusLine);
        }

        return statusCollection;
    }


    public static String getStatusLine(Status status) {
        String statusLine = "@" + status.getUser().getScreenName() + "(" +
                status.getUser().getName() + "): " + status.getText();

        return statusLine;
    }

    public Collection<Status> getHomeTimeline() throws TwitterException {

       // twitter = TwitterFactory.getSingleton();
        List<Status> statuses = twitter.getHomeTimeline();


        logger.info("Showing twitter home timeline.");
        Collection<Status> statusCollection = new ArrayList<Status>();

        for(Status status: statuses) {
            String statusLine = "@" + status.getUser().getScreenName() + "(" +
                    status.getUser().getName() + "): " + status.getText();
            statusCollection.add(status);
        }

        return statusCollection;
    }

}
