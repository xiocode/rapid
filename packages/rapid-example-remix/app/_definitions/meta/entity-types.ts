import type {
  BugLevel,
  EnabledDisabledState,
  TaskState,
} from "./data-dictionary-types";
/**
 * 班次
 */
export interface BaseShift {
  /**
   * id
   */
  id: number;
  /**
   * 名称
   */
  name: string;
  /**
   * 开始时间
   */
  beginTime: string;
  /**
   * 结束时间
   */
  endTime: string;
  /**
   * 创建时间
   */
  createdAt?: string;
  /**
   * 创建人
   */
  createdBy?: Partial<OcUser>;
  /**
   * 更新时间
   */
  updatedAt?: string;
  /**
   * 更新人
   */
  updatedBy?: Partial<OcUser>;
  /**
   * 删除时间
   */
  deletedAt?: string;
  /**
   * 删除人
   */
  deletedBy?: Partial<OcUser>;
}

/**
 * 班次
 */
export type SaveBaseShiftInput = Omit<BaseShift, 'id' | 'createdAt' | 'createdBy' | 'updatedAt' | 'updatedBy'>;

/**
 * 部门
 */
export interface OcDepartment {
  /**
   * id
   */
  id: number;
  /**
   * 上级部门
   */
  parent?: Partial<OcDepartment>;
  /**
   * Code
   */
  code?: string;
  /**
   * 名称
   */
  name?: string;
  /**
   * 排序
   */
  orderNum: number;
  /**
   * 用户
   */
  users?: any;
  /**
   * 创建时间
   */
  createdAt?: string;
  /**
   * 创建人
   */
  createdBy?: Partial<OcUser>;
  /**
   * 更新时间
   */
  updatedAt?: string;
  /**
   * 更新人
   */
  updatedBy?: Partial<OcUser>;
  /**
   * 删除时间
   */
  deletedAt?: string;
  /**
   * 删除人
   */
  deletedBy?: Partial<OcUser>;
}

/**
 * 部门
 */
export type SaveOcDepartmentInput = Omit<OcDepartment, 'id' | 'createdAt' | 'createdBy' | 'updatedAt' | 'updatedBy'>;

/**
 * 角色
 */
export interface OcRole {
  /**
   * id
   */
  id: number;
  /**
   * 名称
   */
  name: string;
  /**
   * 描述
   */
  description?: string;
  /**
   * 排序
   */
  orderNum: number;
  /**
   * 状态
   */
  state: EnabledDisabledState;
  /**
   * 用户
   */
  users?: any;
  /**
   * 操作
   */
  actions?: any;
  /**
   * 创建时间
   */
  createdAt?: string;
  /**
   * 创建人
   */
  createdBy?: Partial<OcUser>;
  /**
   * 更新时间
   */
  updatedAt?: string;
  /**
   * 更新人
   */
  updatedBy?: Partial<OcUser>;
  /**
   * 删除时间
   */
  deletedAt?: string;
  /**
   * 删除人
   */
  deletedBy?: Partial<OcUser>;
}

/**
 * 角色
 */
export type SaveOcRoleInput = Omit<OcRole, 'id' | 'createdAt' | 'createdBy' | 'updatedAt' | 'updatedBy'>;

/**
 * 用户
 */
export interface OcUser {
  /**
   * id
   */
  id: number;
  /**
   * 姓名
   */
  name: string;
  /**
   * 登录账号
   */
  login: string;
  /**
   * 密码
   */
  password?: string;
  /**
   * 是否隐藏
   */
  hidden: boolean;
  /**
   * 状态
   */
  state: EnabledDisabledState;
  /**
   * Email
   */
  email?: string;
  /**
   * 生日
   */
  birthday?: string;
  /**
   * 部门
   */
  department?: Partial<OcDepartment>;
  /**
   * 角色
   */
  roles?: any;
  /**
   * 创建时间
   */
  createdAt?: string;
  /**
   * 创建人
   */
  createdBy?: Partial<OcUser>;
  /**
   * 更新时间
   */
  updatedAt?: string;
  /**
   * 更新人
   */
  updatedBy?: Partial<OcUser>;
  /**
   * 删除时间
   */
  deletedAt?: string;
  /**
   * 删除人
   */
  deletedBy?: Partial<OcUser>;
}

/**
 * 用户
 */
export type SaveOcUserInput = Omit<OcUser, 'id' | 'createdAt' | 'createdBy' | 'updatedAt' | 'updatedBy'>;

/**
 * Bug
 */
export interface PmBugIssue {
  /**
   * id
   */
  id: number;
  /**
   * Bug等级
   */
  bugLevel?: BugLevel;
}

/**
 * Bug
 */
export type SavePmBugIssueInput = Omit<PmBugIssue, 'id' | 'createdAt' | 'createdBy' | 'updatedAt' | 'updatedBy'>;

/**
 * 任务
 */
export interface PmTask {
  /**
   * id
   */
  id: number;
  /**
   * code
   */
  code: string;
  /**
   * 标题
   */
  title: string;
  /**
   * 描述
   */
  description?: string;
  /**
   * 任务类型
   */
  taskType?: string;
  /**
   * 状态
   */
  state?: TaskState;
  /**
   * 权限
   */
  permissions?: Record<string, any>;
  /**
   * 创建时间
   */
  createdAt?: string;
  /**
   * 创建人
   */
  createdBy?: Partial<OcUser>;
  /**
   * 更新时间
   */
  updatedAt?: string;
  /**
   * 更新人
   */
  updatedBy?: Partial<OcUser>;
  /**
   * 删除时间
   */
  deletedAt?: string;
  /**
   * 删除人
   */
  deletedBy?: Partial<OcUser>;
}

/**
 * 任务
 */
export type SavePmTaskInput = Omit<PmTask, 'id' | 'createdAt' | 'createdBy' | 'updatedAt' | 'updatedBy'>;

/**
 * 系统操作
 */
export interface SysAction {
  /**
   * id
   */
  id: number;
  /**
   * 分组
   */
  group: Partial<SysActionGroup>;
  /**
   * Code
   */
  code: string;
  /**
   * 名称
   */
  name: string;
  /**
   * 描述
   */
  description?: string;
  /**
   * 排序号
   */
  orderNum: number;
  /**
   * 创建时间
   */
  createdAt?: string;
  /**
   * 创建人
   */
  createdBy?: Partial<OcUser>;
  /**
   * 更新时间
   */
  updatedAt?: string;
  /**
   * 更新人
   */
  updatedBy?: Partial<OcUser>;
  /**
   * 删除时间
   */
  deletedAt?: string;
  /**
   * 删除人
   */
  deletedBy?: Partial<OcUser>;
}

/**
 * 系统操作
 */
export type SaveSysActionInput = Omit<SysAction, 'id' | 'createdAt' | 'createdBy' | 'updatedAt' | 'updatedBy'>;

/**
 * 系统操作分组
 */
export interface SysActionGroup {
  /**
   * id
   */
  id: number;
  /**
   * Code
   */
  code?: string;
  /**
   * 名称
   */
  name: string;
  /**
   * 排序号
   */
  orderNum: number;
  /**
   * 创建时间
   */
  createdAt?: string;
  /**
   * 创建人
   */
  createdBy?: Partial<OcUser>;
  /**
   * 更新时间
   */
  updatedAt?: string;
  /**
   * 更新人
   */
  updatedBy?: Partial<OcUser>;
  /**
   * 删除时间
   */
  deletedAt?: string;
  /**
   * 删除人
   */
  deletedBy?: Partial<OcUser>;
}

/**
 * 系统操作分组
 */
export type SaveSysActionGroupInput = Omit<SysActionGroup, 'id' | 'createdAt' | 'createdBy' | 'updatedAt' | 'updatedBy'>;
