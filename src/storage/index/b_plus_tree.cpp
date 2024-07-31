#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  std::cout << guard.PageId() << std::endl;
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  std::cout << header_page_id_ << std::endl;
  std::cout << "root_page->root_page_id_ = " << root_page->root_page_id_ << std::endl;
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

// 得到一个写保护页面
auto Context::GetWritePageGuardAt(BufferPoolManager *bpm, page_id_t page_id) -> WritePageGuard {
  auto itr = write_set_.end();
  for (auto it = write_set_.begin(); it != write_set_.end(); ++it) {
    if ((*it).PageId() == page_id) {
      itr = it;
      break;
    }
  }
  WritePageGuard write_guard;
  if (itr == write_set_.end()) {
    write_guard = bpm->FetchPageWrite(page_id);
    return write_guard;
  }
  write_guard = std::move(*itr);
  write_set_.erase(itr);
  return write_guard;
}

// 得到一个读保护页面
auto Context::GetReadPageGuardAt(BufferPoolManager *bpm, page_id_t page_id) -> ReadPageGuard {
  auto itr = read_set_.end();
  for (auto it = read_set_.begin(); it != read_set_.end(); ++it) {
    if ((*it).PageId() == page_id) {
      itr = it;
      break;
    }
  }
  ReadPageGuard read_guard;
  if (itr == read_set_.end()) {
    read_guard = bpm->FetchPageRead(page_id);
    return read_guard;
  }
  read_guard = std::move(*itr);
  read_set_.erase(itr);
  return read_guard;
}

Context::~Context() {
  write_set_.clear();
  read_set_.clear();
  access_set_.clear();
  root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto *header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // 定义读写页面保护
  Context ctx;
  (void)ctx;
  // 找到key所在的叶子节点
  page_id_t leaf_page_id = GetKeyAt(key, comparator_, ctx);
  if (leaf_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto leaf_page_guard = std::move(ctx.read_set_.back());
  ctx.read_set_.pop_back();
  auto *leaf_page = leaf_page_guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  //找到key所在叶子节点中的位置
  int i = leaf_page->Lookup(key, comparator_);
  bool is_success = false;
  if (i >= 0 && i < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(i), key) == 0) {
    if (result != nullptr) {
      result->push_back(leaf_page->ValueAt(i));
    }
    is_success = true;
  }
  if (!is_success) {
    is_success = false;
  }
  return is_success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

// 得到key所在的叶子节点
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetKeyAt(const KeyType &key, const KeyComparator &comparator, Context &ctx) -> page_id_t {
  ReadPageGuard page_guard = bpm_->FetchPageRead(header_page_id_);
  auto *p_header_page = page_guard.As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = p_header_page->root_page_id_;
  ctx.root_page_id_ = root_page_id;
  ctx.read_set_.push_back(std::move(page_guard));
  if (root_page_id == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  ReadPageGuard root_page_guard = bpm_->FetchPageRead(root_page_id);
  auto *root_page = root_page_guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  ctx.access_set_.push_back(root_page_id);
  ctx.read_set_.push_back(std::move(root_page_guard));
  while (!root_page->IsLeafPage()) {
    int i = root_page->Lookup(key, comparator);
    if (i != root_page->GetSize() && comparator(key, root_page->KeyAt(i)) == 0) {
      root_page_id = root_page->GetValue(i);
    } else {
      root_page_id = root_page->GetValue(i - 1);
    }
    root_page_guard = bpm_->FetchPageRead(root_page_id);
    root_page = root_page_guard.As<BPlusTree::InternalPage>();
    ctx.read_set_.pop_back();
    ctx.read_set_.push_back(std::move(root_page_guard));
    ctx.access_set_.push_back(root_page_id);
  }
  return root_page_id;
}

// 得到key所在的叶子节点
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertGetKeyAt(const KeyType &key, const KeyComparator &comparator, Context &ctx) -> page_id_t {
  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = header_page_guard.template AsMut<BPlusTreeHeaderPage>();
  ctx.header_page_ = std::move(header_page_guard);
  page_id_t root_page_id = header_page->root_page_id_;
  if (root_page_id == INVALID_PAGE_ID) {
    bpm_->NewPageGuarded(&root_page_id);
    auto write_guard = bpm_->FetchPageWrite(root_page_id);
    auto *p_leaf_page = write_guard.AsMut<BPlusTree::LeafPage>();
    p_leaf_page->SetPageType(IndexPageType::LEAF_PAGE);
    p_leaf_page->SetMaxSize(leaf_max_size_);
    p_leaf_page->SetNextPageId(INVALID_PAGE_ID);
    p_leaf_page->SetSize(0);
    SetRootPageId(root_page_id, ctx);
    ctx.write_set_.push_back(std::move(write_guard));
    ctx.access_set_.push_back(root_page_id);
    ctx.root_page_id_ = root_page_id;
    // std::cout << "header_page_id_ = " << header_page_id_ << " root_page_id " << root_page_id << std::endl;
    return root_page_id;
  }
  ctx.root_page_id_ = root_page_id;
  WritePageGuard root_page_guard = bpm_->FetchPageWrite(root_page_id);
  auto *root_page = root_page_guard.AsMut<BPlusTree::InternalPage>();
  ctx.access_set_.push_back(root_page_id);
  ctx.write_set_.push_back(std::move(root_page_guard));
  while (!root_page->IsLeafPage()) {
    int i = root_page->Lookup(key, comparator);
    if (i != root_page->GetSize() && comparator(key, root_page->KeyAt(i)) == 0) {
      root_page_id = root_page->GetValue(i);
    } else {
      root_page_id = root_page->GetValue(i - 1);
    }
    root_page_guard = bpm_->FetchPageWrite(root_page_id);
    root_page = root_page_guard.AsMut<BPlusTree::InternalPage>();
    if (root_page->GetSize() + 1 < root_page->GetMaxSize()) {
      if (ctx.header_page_ != std::nullopt) {
        ctx.header_page_.reset();
      }
      ctx.write_set_.clear();
    }
    ctx.write_set_.push_back(std::move(root_page_guard));
    ctx.access_set_.push_back(root_page_id);
  }
  return root_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  Context ctx;
  (void)ctx;
  bool is_success;
  // 找到插入key的叶子节点
  page_id_t leaf_page_id = InsertGetKeyAt(key, comparator_, ctx);
  WritePageGuard leaf_page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto *leaf_page = leaf_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  // 找到key在叶子节点中的位置
  int index = leaf_page->Lookup(key, comparator_);

  if (index > 0 && index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(index), key) == 0) {
    // 已经存在
    is_success = false;
  } else {
    // 如果有足够的空间，直接插入
    if (leaf_page->GetSize() + 1 < leaf_page->GetMaxSize()) {
      leaf_page->Insert(key, value, comparator_);
    } else {
      // 如果没有足够的空间，则新建一个页面，将原来的页面分成两半，将key插入到合适的位置
      page_id_t leaf_page_id_new;
      bpm_->NewPageGuarded(&leaf_page_id_new);
      auto leaf_page_new_guard = bpm_->FetchPageWrite(leaf_page_id_new);
      auto *leaf_page_new = leaf_page_new_guard.template AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
      leaf_page_new->SetMaxSize(leaf_max_size_);
      leaf_page_new->SetSize(0);
      leaf_page_new->SetPageType(IndexPageType::LEAF_PAGE);
      leaf_page_new->SetNextPageId(leaf_page->GetNextPageId());
      // if(maxsize() == 5 )leaf_page_new.size() = 2;
      // if(maxsize() == 6 )leaf_page_new.size() = 3;  
      leaf_page->MoveHalfTo(leaf_page_new);
      leaf_page->SetNextPageId(leaf_page_id_new);
      if (index <= (leaf_page->GetMaxSize() - 1) / 2) {
        leaf_page->Insert(key, value, comparator_);
      } else {
        leaf_page_new->Insert(key, value, comparator_);
      }
      KeyType mid_key = leaf_page_new->KeyAt(0);
      InsertInParent(leaf_page_id, mid_key, leaf_page_id_new, ctx);
    }
    is_success = true;
  }
  return is_success;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetParentPageId(page_id_t child, Context &ctx) -> page_id_t {
  page_id_t parent_id = INVALID_PAGE_ID;
  for (auto it = ctx.access_set_.begin(); (*it) != child; it++) {
    parent_id = (*it);
  }
  return parent_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(page_id_t leaf_page_left_id, KeyType key, page_id_t leaf_page_right_id,
                                    Context &ctx) {
  page_id_t root_page_id = ctx.root_page_id_;

  // 如果当前只有一个节点
  if (root_page_id == leaf_page_left_id) {
    page_id_t root_page_new_id;
    bpm_->NewPageGuarded(&root_page_new_id);

    auto root_page_new_guard = bpm_->FetchPageWrite(root_page_new_id);
    auto *root_page_new =
        root_page_new_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    root_page_new->SetPageType(IndexPageType::INTERNAL_PAGE);
    root_page_new->SetMaxSize(internal_max_size_);
    root_page_new->SetSize(0);
    root_page_new->InsertFirstOf(leaf_page_left_id);
    root_page_new->Insert(key, leaf_page_right_id, comparator_);
    SetRootPageId(root_page_new_id, ctx);
  } else {
    page_id_t parent_page_id = GetParentPageId(leaf_page_left_id, ctx);
    auto parent_page_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto *parent_page = parent_page_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (parent_page->GetSize() < parent_page->GetMaxSize()) {
      // 够就直接插入
      parent_page->Insert(key, leaf_page_right_id, comparator_);
    } else {
      // 内部节点split
      int index = parent_page->Lookup(key, comparator_);
      page_id_t parent_page_new_id;
      bpm_->NewPageGuarded(&parent_page_new_id);
      auto parent_page_new_guard = bpm_->FetchPageWrite(parent_page_new_id);
      auto *parent_page_new = parent_page_new_guard.AsMut<BPlusTree::InternalPage>();
      parent_page_new->SetPageType(IndexPageType::INTERNAL_PAGE);
      parent_page_new->SetMaxSize(internal_max_size_);
      parent_page_new->SetSize(0);
      parent_page->MoveHalfTo(parent_page_new);
      if (index > parent_page->GetMaxSize() / 2) {
        parent_page_new->Insert(key, leaf_page_right_id, comparator_);
      } else {
        parent_page->Insert(key, leaf_page_right_id, comparator_);
      }
      KeyType mid_key = parent_page_new->KeyAt(1);
      page_id_t mid_page_id = parent_page_new->ValueAt(1);
      parent_page_new->EraseAt(1);
      parent_page_new->EraseAt(0);
      parent_page_new->InsertFirstOf(mid_page_id);
      InsertInParent(parent_page_id, mid_key, parent_page_new_id, ctx);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  Context ctx;
  (void)ctx;
  page_id_t leaf_page_id = DeleteGetKeyAt(key, comparator_, ctx);
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  RemoveEntry(leaf_page_id, key, ctx);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteGetKeyAt(const KeyType &key, const KeyComparator &comparator, Context &ctx) -> page_id_t {
  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = header_page_guard.template AsMut<BPlusTreeHeaderPage>();
  ctx.header_page_ = std::move(header_page_guard);
  page_id_t root_page_id = header_page->root_page_id_;
  ctx.root_page_id_ = root_page_id;
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  WritePageGuard root_page_guard = bpm_->FetchPageWrite(root_page_id);
  auto *root_page = root_page_guard.AsMut<BPlusTree::InternalPage>();
  ctx.access_set_.push_back(root_page_id);
  ctx.write_set_.push_back(std::move(root_page_guard));
  while (!root_page->IsLeafPage()) {
    int i = root_page->Lookup(key, comparator);
    if (i != root_page->GetSize() && comparator(key, root_page->KeyAt(i)) == 0) {
      root_page_id = root_page->GetValue(i);
    } else {
      root_page_id = root_page->GetValue(i - 1);
    }
    root_page_guard = bpm_->FetchPageWrite(root_page_id);
    root_page = root_page_guard.AsMut<BPlusTree::InternalPage>();
    // 安全状态，释放之前的页面锁
    if (root_page->GetSize() - 1 >= root_page->GetMinSize()) {
      ctx.header_page_.reset();
      ctx.write_set_.clear();
    }
    ctx.write_set_.push_back(std::move(root_page_guard));
    ctx.access_set_.push_back(root_page_id);
  }
  return root_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetSiblingPageId(const BPlusTree::InternalPage *parent_page, const KeyType &key, Context &ctx)
    -> std::pair<page_id_t, KeyType> {
  page_id_t sibling_page_id;
  KeyType parent_key;
  int index = parent_page->Lookup(key, comparator_);
  int n = parent_page->GetSize();
  if (index == n) {
    sibling_page_id = parent_page->ValueAt(index - 2);
    parent_key = parent_page->KeyAt(index - 1);
  } else if (index > 1 && index <= n - 1) {
    if (comparator_(key, parent_page->KeyAt(index)) == 0) {
      sibling_page_id = parent_page->ValueAt(index - 1);
      parent_key = parent_page->KeyAt(index);
    } else {
      sibling_page_id = parent_page->ValueAt(index - 2);
      parent_key = parent_page->KeyAt(index - 1);
    }
  } else {
    if (comparator_(key, parent_page->KeyAt(index)) == 0) {
      sibling_page_id = parent_page->ValueAt(index - 1);
      parent_key = parent_page->KeyAt(index);
    } else {
      sibling_page_id = parent_page->ValueAt(index);
      parent_key = parent_page->KeyAt(index);
    }
  }
  return std::make_pair(sibling_page_id, parent_key);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReplaceKeyAt(BPlusTree::InternalPage *page, const KeyType &src, const KeyType &dst, Context &ctx) {
  int index = page->Lookup(src, comparator_);
  BUSTUB_ASSERT(!(index < 0 || index >= page->GetSize()), "ReplaceKeyAt page_id source_key not in page");
  page->SetKeyAt(index, dst);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntry(page_id_t basic_page_id, const KeyType &key, Context &ctx) {
  WritePageGuard basic_page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto *basic_page = basic_page_guard.AsMut<BPlusTreePage>();
  bool is_success = true;
  if (basic_page->IsLeafPage()) {
    auto *leaf_page = basic_page_guard.AsMut<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    is_success = leaf_page->RemoveKeyAt(key, comparator_);
  } else {
    auto *internal_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
    is_success = internal_page->RemoveKeyAt(key, comparator_);
  }
  if (!is_success) {
    // key不存在
    return;
  }
  int root_page_id = ctx.root_page_id_;
  if (basic_page_id == root_page_id && basic_page->GetSize() == 0) {
    // 如果根节点为空，那么将根节点删除
    SetTreeEmpty(ctx);
    bpm_->DeletePage(root_page_id);
  } else if (basic_page_id == root_page_id && basic_page->GetSize() == 1 && !basic_page->IsLeafPage()) {
    // 如果根节点只有一个子节点，那么将根节点删除，将子节点作为根节点
    auto *root_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
    SetRootPageId(root_page->ValueAt(0), ctx);
    bpm_->DeletePage(root_page_id);
  } else if(basic_page->GetSize() >= basic_page->GetMinSize()) {
    // 如果删除后节点的size大于等于minsize，直接返回
    return;
  } else if (basic_page_id != root_page_id && basic_page->GetSize() < basic_page->GetMinSize()) {
    // 考虑需要借还是需要合并
    page_id_t parent_page_id = GetParentPageId(basic_page_id, ctx);
    BPlusTree::InternalPage *parent_page;
    WritePageGuard parent_page_guard;
    if (parent_page_id != INVALID_PAGE_ID) {
      parent_page_guard = std::move(ctx.write_set_.back());
      ctx.write_set_.pop_back();
      parent_page = parent_page_guard.AsMut<BPlusTree::InternalPage>();
    }
    auto pair = GetSiblingPageId(parent_page, key, ctx);
    // 兄弟节点的父亲的值
    KeyType mid_key = pair.second;
    page_id_t sibling_id = pair.first;
    WritePageGuard sibling_page_guard = bpm_->FetchPageWrite(sibling_id);
    auto *sibling_page = sibling_page_guard.AsMut<BPlusTreePage>();
    if (sibling_page->GetSize() - 1 < sibling_page->GetMinSize()) {
      // 兄弟不够借
      int index = parent_page->Lookup(key, comparator_);
      if (index == 1 && comparator_(key, parent_page->KeyAt(1)) < 0) {
        // 兄弟节点在自己的右边，交换到左边处理
        std::swap(basic_page, sibling_page);
        WritePageGuard tmp = std::move(basic_page_guard);
        basic_page_guard = std::move(sibling_page_guard);
        sibling_page_guard = std::move(tmp);
        std::swap(basic_page_id, sibling_id);
      }
      if (!basic_page->IsLeafPage()) {
        auto *basic_internal_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
        auto *sibling_internal_page = sibling_page_guard.AsMut<BPlusTree::InternalPage>();
        page_id_t mid_key_page_id = basic_internal_page->ValueAt(0);
        sibling_internal_page->Insert(mid_key, mid_key_page_id, comparator_);
        basic_internal_page->MoveAllTo(sibling_internal_page);
      } else {
        auto *basic_leaf_page = basic_page_guard.AsMut<BPlusTree::LeafPage>();
        auto *sibling_leaf_page = sibling_page_guard.AsMut<BPlusTree::LeafPage>();
        basic_leaf_page->MoveAllTo(sibling_leaf_page);
        sibling_leaf_page->SetNextPageId(basic_leaf_page->GetNextPageId());
      }
      ctx.write_set_.push_back(std::move(parent_page_guard));
      RemoveEntry(parent_page_id, mid_key, ctx);
      bpm_->DeletePage(basic_page_id);
    } else {
      // 兄弟够借
      int index = parent_page->Lookup(key, comparator_);
      if (index == 1 && comparator_(key, parent_page->KeyAt(1)) < 0) {
        // 兄弟节点在自己的右边
        if (!basic_page->IsLeafPage()) {
          auto *basic_internal_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
          auto *sibling_internal_page = sibling_page_guard.AsMut<BPlusTree::InternalPage>();
          int m = 0;
          page_id_t first_page_id = sibling_internal_page->ValueAt(m);
          KeyType first_key = sibling_internal_page->KeyAt(m + 1);
          basic_internal_page->Insert(mid_key, first_page_id, comparator_);
          sibling_internal_page->EraseAt(0);
          sibling_internal_page->SetKeyAt(0, KeyType());
          ReplaceKeyAt(parent_page, mid_key, first_key, ctx);
        } else {
          auto *basic_leaf_page = basic_page_guard.AsMut<BPlusTree::LeafPage>();
          auto *sibling_leaf_page = sibling_page_guard.AsMut<BPlusTree::LeafPage>();
          sibling_leaf_page->MoveFirstToEndOf(basic_leaf_page);
          KeyType second_key = sibling_leaf_page->KeyAt(0);
          ReplaceKeyAt(parent_page, mid_key, second_key, ctx);
        }
      } else {
        // 兄弟节点在自己的左边
        if (!basic_page->IsLeafPage()) {
          auto *basic_internal_page = basic_page_guard.AsMut<BPlusTree::InternalPage>();
          auto *sibling_internal_page = sibling_page_guard.AsMut<BPlusTree::InternalPage>();
          int m = sibling_internal_page->GetSize() - 1;
          page_id_t last_page_id = sibling_internal_page->ValueAt(m);
          KeyType last_key = sibling_internal_page->KeyAt(m);
          sibling_internal_page->EraseAt(m);
          page_id_t basic_pointer_page_id = basic_internal_page->ValueAt(0);
          basic_internal_page->SetValueAt(0, last_page_id);
          basic_internal_page->Insert(mid_key, basic_pointer_page_id, comparator_);
          ReplaceKeyAt(parent_page, mid_key, last_key, ctx);
        } else {
          auto *basic_leaf_page = basic_page_guard.AsMut<BPlusTree::LeafPage>();
          auto *sibling_leaf_page = sibling_page_guard.AsMut<BPlusTree::LeafPage>();
          int m = sibling_leaf_page->GetSize() - 1;
          ValueType last_value = sibling_leaf_page->ValueAt(m);
          KeyType last_key = sibling_leaf_page->KeyAt(m);
          sibling_leaf_page->RemoveAt(m);
          basic_leaf_page->Insert(last_key, last_value, comparator_);
          ReplaceKeyAt(parent_page, mid_key, last_key, ctx);
        }
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetTreeEmpty(Context &ctx) {
  auto header_page = std::move(ctx.header_page_);
  auto *p_header_page = header_page->AsMut<BPlusTreeHeaderPage>();
  p_header_page->root_page_id_ = INVALID_PAGE_ID;
  ctx.header_page_ = std::move(header_page);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Context ctx;
  auto root_page_id = GetRootPageId();
  if (root_page_id == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  BasicPageGuard root_page_guard = bpm_->FetchPageBasic(root_page_id);
  auto *root_page = root_page_guard.As<BPlusTree::InternalPage>();
  while (!root_page->IsLeafPage()) {
    root_page_id = root_page->ValueAt(0);
    if (root_page_id != INVALID_PAGE_ID) {
      root_page_guard = bpm_->FetchPageBasic(root_page_id);
      root_page = root_page_guard.As<BPlusTree::InternalPage>();
    }
  }
  auto *leaf_page = root_page_guard.As<BPlusTree::LeafPage>();
  return INDEXITERATOR_TYPE(bpm_, leaf_page, 0, std::move(root_page_guard));
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  (void)ctx;
  auto page_id = GetKeyAt(key, comparator_, ctx);
  ctx.read_set_.clear();
  if (page_id == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE();
  }
  BasicPageGuard leaf_page_guard = bpm_->FetchPageBasic(page_id);
  ctx.read_set_.pop_back();
  const auto *leaf_page = leaf_page_guard.As<BPlusTree::LeafPage>();
  int index = leaf_page->Lookup(key, comparator_);
  if (comparator_(leaf_page->KeyAt(index), key) != 0) {
    return INDEXITERATOR_TYPE();
  }
  return INDEXITERATOR_TYPE(bpm_, leaf_page, index, std::move(leaf_page_guard));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(nullptr, nullptr, -1, BasicPageGuard()); }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPageId(page_id_t page_id, Context &ctx) {
  auto guard = std::move(ctx.header_page_);
  auto *header_page = guard->AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = page_id;
  ctx.root_page_id_ = page_id;
  ctx.header_page_ = std::move(guard);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard page_guard = bpm_->FetchPageRead(header_page_id_);
  auto *p_header_page = page_guard.As<BPlusTreeHeaderPage>();
  return p_header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
