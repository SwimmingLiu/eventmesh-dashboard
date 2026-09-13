package org.apache.eventmesh.dashboard.console.utils.data.adaptation.eventmesh.topic;

import org.apache.eventmesh.dashboard.console.entity.message.TopicEntity;
import org.apache.eventmesh.dashboard.console.model.dto.topic.CreateTopicDTO;
import org.apache.eventmesh.dashboard.console.utils.data.adaptation.eventmesh.EventmeshMQAdaptation;
import org.apache.eventmesh.dashboard.console.utils.data.adaptation.operation.UpdateAdaptation;
import org.apache.eventmesh.dashboard.console.utils.data.adaptation.type.TopicAdaptation;

/**
 * @author hahaha
 */
public class EventMeshCreateUpdateAdaptation implements TopicAdaptation<CreateTopicDTO>,
    EventmeshMQAdaptation<TopicEntity, CreateTopicDTO>,
    UpdateAdaptation<TopicEntity, CreateTopicDTO> {

    @Override
    public void adaptation(TopicEntity topicEntity, CreateTopicDTO createTopicDTO) {

    }
}
