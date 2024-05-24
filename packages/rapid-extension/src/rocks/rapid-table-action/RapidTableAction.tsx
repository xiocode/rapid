import { Rock, RockConfig, RockEvent, handleComponentEvent } from "@ruiapp/move-style";
import RapidToolbarMeta from "./RapidTableActionMeta";
import { renderRock } from "@ruiapp/react-renderer";
import { RapidTableActionRockConfig } from "./rapid-table-action-types";
import { Modal } from "antd";


export default {
  $type: "rapidTableAction",

  Renderer(context, props) {
    const {record, recordId, actionText, confirmText, onAction } = props;
    const rockConfig: RockConfig = {
      $id: `${props.$id}-anchor`,
      $type: 'anchor',
      className: "rui-table-action-link",
      "data-record-id": recordId,
      children: {
        $type: "text",
        text: actionText,
      },
    };

    if (onAction) {
      rockConfig.onClick = [
        {
          $action: "script",
          script: (event: RockEvent) => {
            if (confirmText) {
              Modal.confirm({
                title: confirmText,
                onOk: async () => {
                  handleComponentEvent("onAction", event.framework, event.page as any, event.scope, event.sender, onAction, [record]);
                },
              });
            } else {
              handleComponentEvent("onAction", event.framework, event.page as any, event.scope, event.sender, onAction, [record]);
            }
          },
        },
      ];
    }

    return renderRock({context, rockConfig});
  },

  ...RapidToolbarMeta,
} as Rock<RapidTableActionRockConfig>;