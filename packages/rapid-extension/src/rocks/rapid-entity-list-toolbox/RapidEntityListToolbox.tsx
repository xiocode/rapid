import type { Rock, SimpleRockConfig } from "@ruiapp/move-style";
import { get, isArray, isEmpty, keyBy, set, split } from "lodash";
import { Checkbox, Popover, Space, Tree } from "antd";
import { ReloadOutlined, SettingOutlined } from "@ant-design/icons";
import { useMemo, useState } from "react";
import { RapidExtStorage } from "../../utils/storage-utility";
import { convertToEventHandlers } from "@ruiapp/react-renderer";
import { RapidTableColumnConfig } from "../rapid-table-column/rapid-table-column-types";

import "./style.css";

export interface IRapidEntityListToolboxConfig {
  columnCacheKey?: string;
}

export interface ICacheRapidTableColumn extends Pick<RapidTableColumnConfig, "code" | "key"> {
  hidden?: boolean;
}

interface IProps extends SimpleRockConfig {
  config: IRapidEntityListToolboxConfig;
  columns: any[];
  onRerender?(): void;
  onReload?(): void;
}

export default {
  $type: "rapidEntityListToolbox",

  slots: {},

  propertyPanels: [],

  Renderer(context, props: IProps) {
    const { columns } = props;
    const { columnCacheKey } = props.config;

    const [checkedKeys, setCheckedKeys] = useState<string[]>(() => {
      const cacheColumns = RapidExtStorage.get<ICacheRapidTableColumn[]>(columnCacheKey);

      if (isArray(cacheColumns) && !isEmpty(cacheColumns)) {
        const cacheColumnByCodeMap = keyBy<ICacheRapidTableColumn>(cacheColumns, getColumnUniqueKey);
        return (columns || []).filter((col) => !cacheColumnByCodeMap[col.code]?.hidden).map(getColumnUniqueKey);
      }

      return (columns || []).map(getColumnUniqueKey);
    });

    const eventHandlers = convertToEventHandlers({ context, rockConfig: props }) as any;

    const onTreeCheck = (checkedKeys: string[]) => {
      const checkedColumns = (columns || []).map<ICacheRapidTableColumn>((col) => ({ ...col, hidden: !checkedKeys.includes(getColumnUniqueKey(col)) }));

      setCheckedKeys(checkedKeys);

      RapidExtStorage.set<ICacheRapidTableColumn[]>(
        columnCacheKey,
        checkedColumns.map((col) => ({ code: col.code, key: col.key, hidden: col.hidden })),
      );
      eventHandlers.onRerender?.();
    };

    const onTreeDrop = ({ node, dragNode }) => {
      const cacheColumns = RapidExtStorage.get<ICacheRapidTableColumn[]>(columnCacheKey);

      const nodeIndex = parseInt(split(node.pos, "-")[1], 10);
      const dragNodeIndex = parseInt(split(dragNode.pos, "-")[1], 10);

      const originColumns = [...(columns || [])];
      const dragColumn = originColumns[dragNodeIndex];
      if (nodeIndex < dragNodeIndex) {
        originColumns.splice(dragNodeIndex, 1);
        originColumns.splice(nodeIndex === 0 ? 0 : nodeIndex + 1, 0, dragColumn);
      } else if (nodeIndex > dragNodeIndex) {
        originColumns.splice(nodeIndex + 1, 0, dragColumn);
        originColumns.splice(dragNodeIndex, 1);
      }

      const cacheColumnByCodeMap = keyBy<ICacheRapidTableColumn>(cacheColumns, getColumnUniqueKey);
      RapidExtStorage.set<ICacheRapidTableColumn[]>(
        columnCacheKey,
        originColumns.map((col) => {
          const uniqueKey = getColumnUniqueKey(col);
          const cacheColumn = cacheColumnByCodeMap[uniqueKey];
          return { code: col.code, key: col.key, hidden: cacheColumn?.hidden || false };
        }),
      );
      eventHandlers.onRerender?.();
    };

    return (
      <div className="rapid-entity-list-toolbar">
        <div></div>
        <Space size={16}>
          <span
            className="rapid-entity-list-toolbar--iconBtn"
            onClick={(e) => {
              e.stopPropagation();

              if (typeof eventHandlers.onReload === "function") {
                eventHandlers.onReload();
                return;
              }

              context.scope.loadStoreData("list", {});
            }}
          >
            <ReloadOutlined />
          </span>
          <Popover
            showArrow={false}
            trigger={["click"]}
            placement="leftTop"
            content={
              <div className="rapid-entity-list-toolbar-settings">
                <Tree draggable blockNode selectable={false} checkable checkedKeys={checkedKeys} onCheck={onTreeCheck} onDrop={onTreeDrop}>
                  {(columns || []).map((col) => (
                    <Tree.TreeNode key={getColumnUniqueKey(col)} title={col.title} />
                  ))}
                </Tree>
              </div>
            }
          >
            <span className="rapid-entity-list-toolbar--iconBtn">
              <SettingOutlined />
            </span>
          </Popover>
        </Space>
      </div>
    );
  },
} as Rock;

export function getColumnUniqueKey(column: RapidTableColumnConfig): string {
  return column?.key || column?.code;
}
